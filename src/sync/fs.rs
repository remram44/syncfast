//! Synchronization from and to local files.

use cdchunking::{Chunker, ZPAQ};
use futures::channel::mpsc::{Receiver, channel};
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
    file: &mut File,
    offset: usize,
    block: &[u8],
) -> Result<(), Error> {
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
                    receiver: receiver,
                    state: FsSourceState::ListFiles(None),
                }),
                fssourcefrom_stream,
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
    receiver: Receiver<DestinationEvent>,
    state: FsSourceState,
}

impl<'a> FsSourceFrom<'a> {
    fn project<'b>(self: &'b mut Pin<Box<Self>>) -> (&'b mut Index, Pin<&'b mut Receiver<DestinationEvent>>, &'b mut FsSourceState) where 'a: 'b {
        unsafe {
            let s = self.as_mut().get_unchecked_mut();
            (
                s.index,
                Pin::new_unchecked(&mut s.receiver),
                &mut s.state,
            )
        }
    }
}

fn fssourcefrom_stream(mut stream: Pin<Box<FsSourceFrom>>) -> impl Future<Output=Option<(Result<SourceEvent, Error>, Pin<Box<FsSourceFrom>>)>> {
    async {
        let (index, mut receiver, state) = stream.project();

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
                        let data = try_!(read_block(&path, offset));
                        Some((Ok(SourceEvent::BlockData(data)), stream))
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
        todo!() // FsDestination
    }
}
