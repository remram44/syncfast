//! Synchronization from and to local files.

use cdchunking::{Chunker, ZPAQ};
use futures::channel::mpsc::{Receiver, Sender, channel};
use futures::sink::{Sink, SinkExt};
use futures::stream::{LocalBoxStream, StreamExt};
use log::{debug, info, warn};
use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::future::Future;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::pin::Pin;

use crate::{Error, HashDigest};
use crate::index::{MAX_BLOCK_SIZE, ZPAQ_BITS, Index};
use crate::sync::{Destination, DestinationEvent, Source, SourceEvent};

fn read_block(path: &Path, offset: usize) -> Result<Vec<u8>, Error> {
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(offset as u64))?;
    let chunker = Chunker::new(
        ZPAQ::new(ZPAQ_BITS),
    ).max_size(MAX_BLOCK_SIZE);
    let block = chunker.whole_chunks(file).next().unwrap()?;
    Ok(block)
}

fn write_block(
    name: &Path,
    offset: usize,
    block: &[u8],
) -> Result<(), Error> {
    let mut file = OpenOptions::new().write(true).create(true).open(name)?;
    file.seek(SeekFrom::Start(offset as u64))?;
    file.write_all(block)?;
    Ok(())
}

pub struct FsSource {
    index: Index,
    root_dir: PathBuf,
}

impl FsSource {
    /// Create a source from the (source) index
    pub fn new(
        root_dir: PathBuf,
    ) -> Result<FsSource, Error> {
        let mut index = Index::open(&root_dir.join(".syncfast.idx"))?;
        info!("Indexing source into {:?}...", root_dir.join(".syncfast.idx"));
        index.index_path(&root_dir)?;
        index.remove_missing_files(&root_dir)?;
        index.commit()?;
        Ok(FsSource {
            index,
            root_dir,
        })
    }
}

impl Source for FsSource {
    fn streams<'a>(&'a mut self) -> (LocalBoxStream<'a, Result<SourceEvent, Error>>, Pin<Box<dyn Sink<DestinationEvent, Error=Error> + 'a>>) {
        let (sender, receiver) = channel(1);
        (
            futures::stream::unfold(
                Box::pin(FsSourceFrom {
                    index: &mut self.index,
                    root_dir: &self.root_dir,
                    receiver: receiver,
                    state: FsSourceState::ListFiles(None),
                }),
                FsSourceFrom::stream,
            ).boxed_local(),
            Box::pin(futures::sink::unfold((), move |_, event: DestinationEvent| {
                let mut sender = sender.clone();
                async move {
                    sender.send(event).await.expect("fs channel");
                    Ok(())
                }
            })),
        )
    }
}

enum FsSourceState {
    ListFiles(Option<VecDeque<(Vec<u8>, usize, HashDigest)>>),
    Respond,
    ListBlocks(VecDeque<(HashDigest, usize)>),
    Done,
}

struct FsSourceFrom<'a> {
    index: &'a mut Index,
    root_dir: &'a Path,
    receiver: Receiver<DestinationEvent>,
    state: FsSourceState,
}

impl<'a> FsSourceFrom<'a> {
    fn project<'b>(self: &'b mut Pin<Box<Self>>) -> (&'b mut Index, &'b Path, Pin<&'b mut Receiver<DestinationEvent>>, &'b mut FsSourceState) where 'a: 'b {
        unsafe {
            let s = self.as_mut().get_unchecked_mut();
            (
                s.index,
                s.root_dir,
                Pin::new_unchecked(&mut s.receiver),
                &mut s.state,
            )
        }
    }

    fn stream(mut stream: Pin<Box<FsSourceFrom>>) -> impl Future<Output=Option<(Result<SourceEvent, Error>, Pin<Box<FsSourceFrom>>)>> {
        async {
            let (index, root_dir, mut receiver, state) = stream.project();

            macro_rules! err {
                ($e:expr) => {
                    Some((Err($e), stream))
                }
            }
            // FIXME: Replace by try_block when supported by Rust
            macro_rules! try_ {
                ($v:expr) => {
                    match $v {
                        Ok(r) => r,
                        Err(e) => return err!(e),
                    }
                }
            }

            match *state {
                // Send files list
                FsSourceState::ListFiles(ref mut list) => {
                    // If we don't have data, fetch from database
                    if list.is_none() {
                        // FIXME: Don't get all files at once, iterate
                        let files = try_!(index.list_files());
                        let mut new_list = VecDeque::with_capacity(files.len());
                        for (_file_id, path, _modified, size, blocks_hash) in files {
                            let path = path
                                .into_os_string()
                                .into_string()
                                .expect("encoding")
                                .into_bytes();
                            new_list.push_back((path, size as usize, blocks_hash));
                        }
                        *list = Some(new_list);
                    }
                    let list = list.as_mut().unwrap();
                    match list.pop_front() {
                        Some((path, size, blocks_hash)) => Some((Ok(SourceEvent::FileEntry(path, size, blocks_hash)), stream)),
                        None => {
                            *state = FsSourceState::Respond;
                            Some((Ok(SourceEvent::EndFiles), stream))
                        }
                    }
                }
                // Files are sent, respond to requests
                FsSourceState::Respond => {
                    let req = match receiver.as_mut().next().await {
                        None => return None,
                        Some(e) => e,
                    };
                    match req {
                        DestinationEvent::GetFile(path) => {
                            let path_str = String::from_utf8(path).expect("encoding");
                            let (file_id, _modified, _blocks_hash) = match try_!(index.get_file(Path::new(&path_str))) {
                                Some(t) => t,
                                None => return err!(Error::Io(std::io::Error::new(std::io::ErrorKind::NotFound, "Requested file is unknown"))),
                            };
                            // FIXME: Don't get all blocks at once, iterate
                            let blocks = try_!(index.list_file_blocks(file_id));
                            let mut new_blocks = VecDeque::with_capacity(blocks.len());
                            for (hash, _offset, size) in blocks {
                                new_blocks.push_back((hash, size));
                            }
                            *state = FsSourceState::ListBlocks(new_blocks);
                            Some((Ok(SourceEvent::FileStart(path_str.into_bytes())), stream))
                        }
                        DestinationEvent::GetBlock(hash) => {
                            let (path, offset, _size) = match try_!(index.get_block(&hash)) {
                                Some(t) => t,
                                None => return err!(Error::Io(std::io::Error::new(std::io::ErrorKind::NotFound, "Requested block is unknown"))),
                            };
                            let mut fullpath = root_dir.to_owned();
                            fullpath.push(&path);
                            let data = try_!(read_block(&fullpath, offset));
                            Some((Ok(SourceEvent::BlockData(hash, data)), stream))
                        }
                        DestinationEvent::Complete => {
                            *state = FsSourceState::Done;
                            None
                        }
                    }
                }
                // List blocks
                FsSourceState::ListBlocks(ref mut list) => {
                    match list.pop_front() {
                        Some((hash, size)) => Some((Ok(SourceEvent::FileBlock(hash, size)), stream)),
                        None => {
                            *state = FsSourceState::Respond;
                            Some((Ok(SourceEvent::FileEnd), stream))
                        }
                    }
                }
                // Stream is done
                FsSourceState::Done => None,
            }
        }
    }
}

pub struct FsDestination {
    index: Index,
    root_dir: PathBuf,
}

impl FsDestination {
    /// Create a destination from the (destination) index
    pub fn new(root_dir: PathBuf) -> Result<FsDestination, Error> {
        let mut index = Index::open(&root_dir.join(".syncfast.idx"))?;
        info!(
            "Indexing destination into {:?}...",
            root_dir.join(".syncfast.idx")
        );
        index.index_path(&root_dir)?;
        index.remove_missing_files(&root_dir)?;
        index.commit()?;
        Ok(FsDestination {
            index,
            root_dir,
        })
    }
}

impl Destination for FsDestination {
    fn streams<'a>(&'a mut self) -> (LocalBoxStream<'a, Result<DestinationEvent, Error>>, Pin<Box<dyn Sink<SourceEvent, Error=Error>+ 'a>>) {
        let (sender, receiver) = channel(1);
        (
            futures::stream::unfold(
                receiver,
                |mut r| {
                    async move {
                        match r.next().await {
                            Some(v) => Some((Ok(v), r)),
                            None => None,
                        }
                    }
                },
            ).boxed_local(),
            Box::pin(futures::sink::unfold(
                FsDestinationTo {
                    index: &mut self.index,
                    root_dir: &self.root_dir,
                    sender,
                    state: FsDestinationState::FilesList,
                },
                FsDestinationTo::sink,
            )),
        )
    }
}

enum FsDestinationState {
    FilesList,
    FileBlocks(usize, Option<(u32, usize)>),
    Blocks(usize),
    Done,
}

struct FsDestinationTo<'a> {
    index: &'a mut Index,
    root_dir: &'a Path,
    sender: Sender<DestinationEvent>,
    state: FsDestinationState,
}

impl<'a> FsDestinationTo<'a> {
    fn sink(mut sink: FsDestinationTo, event: SourceEvent) -> impl Future<Output=Result<FsDestinationTo, Error>> {
        async {
            match event {
                SourceEvent::FileEntry(path, _size, blocks_hash) => {
                    match sink.state {
                        FsDestinationState::FilesList => {}
                        _ => Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Unexpected FileEntry"))?,
                    }
                    let path: PathBuf = String::from_utf8(path)
                        .expect("encoding")
                        .into();
                    match sink.index.get_file(&path)? {
                        Some((_file_id, _modified, recorded_blocks_hash)) => {
                            if blocks_hash == recorded_blocks_hash {
                                return Ok(sink); // File is up to date, do nothing
                            }
                        }
                        None => {}
                    };
                    // Create temporary file
                    let (_file_id, temp_path) = sink.index.add_temp_file(&path)?;
                    OpenOptions::new()
                        .write(true)
                        .truncate(true)
                        .create(true)
                        .open(temp_path)?;
                }
                SourceEvent::EndFiles => {
                    match sink.state {
                        FsDestinationState::FilesList => {}
                        _ => Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Unexpected FileEntry"))?,
                    }
                    let mut requested_files = 0;
                    for name in sink.index.list_temp_files()? {
                        let name = name
                            .into_os_string()
                            .into_string()
                            .expect("encoding")
                            .into_bytes();
                        sink.sender
                            .send(DestinationEvent::GetFile(name))
                            .await
                            .map_err(|e| std::io::Error::new(std::io::ErrorKind::BrokenPipe, e))?;
                        requested_files += 1;
                    }
                    sink.state = FsDestinationState::FileBlocks(requested_files, None);
                }
                SourceEvent::FileStart(path) => {
                    let nb_files = match sink.state {
                        FsDestinationState::FileBlocks(nb_files, None) if nb_files > 0 => nb_files,
                        FsDestinationState::FileBlocks(_, Some(_)) => Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "FileStart before FileEnd"))?,
                        FsDestinationState::FileBlocks(_, None) => Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Too many FileStart"))?,
                        _ => Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Unexpected FileStart"))?,
                    };
                    let path: PathBuf = String::from_utf8(path)
                        .expect("encoding")
                        .into();
                    let (file_id, _modified, _blocks_hash) = sink.index
                        .get_file(&path)?
                        .ok_or(std::io::Error::new(std::io::ErrorKind::NotFound, "Unknown file"))?;
                    sink.state = FsDestinationState::FileBlocks(nb_files, Some((file_id, 0)));
                }
                SourceEvent::FileBlock(hash, size) => {
                    match sink.state {
                        FsDestinationState::FileBlocks(_, Some((file_id, ref mut offset))) => {
                            sink.index.add_block(&hash, file_id, *offset, size)?;
                            *offset += size;
                        }
                        _ => Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Unexpected FileBlock"))?,
                    }
                }
                SourceEvent::FileEnd => {
                    match sink.state {
                        FsDestinationState::FileBlocks(mut nb_files, Some(..)) => {
                            nb_files -= 1;
                            sink.state = FsDestinationState::FileBlocks(nb_files, None);
                            if nb_files == 0 {
                                let mut nb_blocks = 0;
                                for hash in sink.index.list_missing_blocks()? {
                                    sink.sender
                                        .send(DestinationEvent::GetBlock(hash))
                                        .await
                                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::BrokenPipe, e))?;
                                    nb_blocks += 1;
                                }
                                sink.state = FsDestinationState::Blocks(nb_blocks);
                            }
                        }
                        _ => Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Unexpected FileEnd"))?,
                    }
                }
                SourceEvent::BlockData(hash, data) => {
                    match sink.state {
                        FsDestinationState::Blocks(mut nb_blocks) => {
                            nb_blocks -= 1;
                            sink.state = FsDestinationState::Blocks(nb_blocks);
                            if nb_blocks == 0 {
                                debug!("FsDestination: send Complete");
                                sink.sender
                                    .send(DestinationEvent::Complete)
                                    .await
                                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::BrokenPipe, e))?;
                                sink.state = FsDestinationState::Done;
                            }
                        }
                        _ => Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Unexpected BlockData"))?,
                    }
                    for (name, offset, _size) in sink.index.list_block_locations(&hash)? {
                        let mut fullpath = sink.root_dir.to_owned();
                        fullpath.push(&name);
                        write_block(&fullpath, offset, &data)?;
                    }
                }
            }
            Ok(sink)
        }
    }
}
