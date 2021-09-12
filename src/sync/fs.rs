//! Synchronization from and to local files.

use cdchunking::{Chunker, ZPAQ};
use futures::channel::mpsc::{Receiver, channel};
use futures::sink::{Sink, SinkExt};
use futures::stream::{LocalBoxStream, StreamExt};
use log::{log_enabled, debug, info};
use log::Level::Debug;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::future::Future;
use std::io::{Seek, SeekFrom, Write};
use std::ops::DerefMut;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::rc::Rc;

use crate::{Error, HashDigest, temp_name, untemp_name};
use crate::index::{MAX_BLOCK_SIZE, ZPAQ_BITS, Index};
use crate::sync::{Destination, DestinationEvent, Source, SourceEvent};
use crate::sync::utils::Condition;

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
    /// Create a source from a directory, indexing it immediately
    pub fn new(
        root_dir: PathBuf,
    ) -> Result<FsSource, Error> {
        info!("Indexing source into {:?}...", root_dir.join(".syncfast.idx"));
        let mut index = Index::open(&root_dir.join(".syncfast.idx"))?;
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
        // The source can't handle multiple input events, so we just implement
        // a Stream, and use a channel for the Sink
        debug!("FsSource: state=ListFiles");
        let (sender, receiver) = channel(1);
        (
            // Stream generating events using FsSourceFrom::stream
            futures::stream::unfold(
                Box::pin(FsSourceFrom {
                    index: &mut self.index,
                    root_dir: &self.root_dir,
                    receiver,
                    state: FsSourceState::ListFiles(None),
                }),
                FsSourceFrom::stream,
            ).boxed_local(),
            // Simple Sink feeding the channel for the Stream to read
            Box::pin(futures::sink::unfold((), move |(), event: DestinationEvent| {
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
        unsafe { // Required for pin projection
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
                        debug!("FsSource: preparing to send {} files", new_list.len());
                        *list = Some(new_list);
                    }
                    let list = list.as_mut().unwrap();
                    match list.pop_front() {
                        Some((path, size, blocks_hash)) => {
                            if log_enabled!(Debug) {
                                debug!("FsSource: send FileEntry({})", String::from_utf8_lossy(&path));
                            }
                            Some((Ok(SourceEvent::FileEntry(path, size, blocks_hash)), stream))
                        }
                        None => {
                            debug!("FsSource: state=Respond");
                            *state = FsSourceState::Respond;
                            debug!("FsSource: send EndFiles");
                            Some((Ok(SourceEvent::EndFiles), stream))
                        }
                    }
                }
                // Files are sent, respond to requests
                FsSourceState::Respond => {
                    let req = match receiver.as_mut().next().await {
                        None => {
                            debug!("FsSource: got end of input");
                            return None;
                        }
                        Some(e) => e,
                    };
                    debug!("FsSource: recv {:?}", req);
                    match req {
                        DestinationEvent::GetFile(path) => {
                            let path_str = String::from_utf8(path).expect("encoding");
                            let (file_id, _modified, _blocks_hash) = match try_!(index.get_file(Path::new(&path_str))) {
                                Some(t) => t,
                                None => return err!(Error::Io(std::io::Error::new(std::io::ErrorKind::NotFound, "Requested file is unknown"))),
                            };
                            debug!("FsSource: file_id={}", file_id);
                            // FIXME: Don't get all blocks at once, iterate
                            let blocks = try_!(index.list_file_blocks(file_id));
                            let mut new_blocks = VecDeque::with_capacity(blocks.len());
                            for (hash, _offset, size) in blocks {
                                new_blocks.push_back((hash, size));
                            }
                            debug!("FsSource: state=ListBlocks");
                            debug!("FsSource: preparing to send {} blocks", new_blocks.len());
                            *state = FsSourceState::ListBlocks(new_blocks);
                            debug!("FsSource: send FileStart");
                            Some((Ok(SourceEvent::FileStart(path_str.into_bytes())), stream))
                        }
                        DestinationEvent::GetBlock(hash) => {
                            let (path, offset, _size) = match try_!(index.get_block(&hash)) {
                                Some(t) => t,
                                None => return err!(Error::Io(std::io::Error::new(std::io::ErrorKind::NotFound, "Requested block is unknown"))),
                            };
                            debug!("FsSource: found block in {:?} offset {}", path, offset);
                            let data = try_!(read_block(&root_dir.join(&path), offset));
                            debug!("FsSource: send BlockData");
                            Some((Ok(SourceEvent::BlockData(hash, data)), stream))
                        }
                        DestinationEvent::Complete => {
                            *state = FsSourceState::Done;
                            debug!("FsSource: state=Done");
                            None
                        }
                    }
                }
                // List blocks
                FsSourceState::ListBlocks(ref mut list) => {
                    match list.pop_front() {
                        Some((hash, size)) => {
                            debug!("FsSource: send FileBlock");
                            Some((Ok(SourceEvent::FileBlock(hash, size)), stream))
                        }
                        None => {
                            debug!("FsSource: out of blocks");
                            debug!("FsSource: state=Respond");
                            *state = FsSourceState::Respond;
                            debug!("FsSource: send FileEnd");
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
    /// Create a destination from a directory, indexing it immediately
    pub fn new(root_dir: PathBuf) -> Result<FsDestination, Error> {
        info!(
            "Indexing destination into {:?}...",
            root_dir.join(".syncfast.idx")
        );
        std::fs::create_dir_all(&root_dir)?;
        let mut index = Index::open(&root_dir.join(".syncfast.idx"))?;
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
        // The destination has to handle input while producing output (for
        // example getting BlockData while sending GetBlock), so it has both a
        // custom Stream and Sink implementations
        // State changes are triggered by Sink
        let destination = Rc::new(RefCell::new(FsDestinationInner {
            index: &mut self.index,
            root_dir: &self.root_dir,
            state: FsDestinationState::FilesList { cond: Default::default() },
        }));
        debug!("FsDestination: state=FilesList");
        (
            // Stream generating events using FsDestination::stream
            futures::stream::unfold(
                destination.clone(),
                FsDestinationInner::stream,
            ).boxed_local(),
            // Sink handling events using FsDestination::sink
            Box::pin(futures::sink::unfold(
                destination,
                FsDestinationInner::sink,
            )),
        )
    }
}

struct FsDestinationInner<'a> {
    index: &'a mut Index,
    root_dir: &'a Path,
    state: FsDestinationState,
}

enum FsDestinationState {
    FilesList {
        /// Sink indicates state change (`SourceEvent::EndFiles`)
        cond: Condition,
    },
    GetFiles {
        /// List of files to request the blocks of
        files_to_request: VecDeque<Vec<u8>>,
        /// Number of files to receive
        files_to_receive: usize,
        /// Sink indicates state change (got `SourceEvent::FileEnd` and no more files_to_request)
        cond: Condition,
        /// file_id and offset for the blocks we're receiving (from previous FileStart)
        file_blocks_id: Option<(u32, usize)>,
    },
    GetBlocks {
        /// List of blocks to request, None if we've sent `DestinationEvent::Complete`
        blocks_to_request: Option<VecDeque<HashDigest>>,
        /// Number of blocks to receive
        blocks_to_receive: usize,
    },
}

impl<'a> FsDestinationInner<'a> {
    fn stream(inner: Rc<RefCell<FsDestinationInner>>) -> impl Future<Output=Option<(Result<DestinationEvent, Error>, Rc<RefCell<FsDestinationInner>>)>> {
        async move {
            loop {
                let ret = match inner.borrow_mut().state {
                    // Receive files list
                    FsDestinationState::FilesList { ref mut cond } => {
                        // Nothing to produce, wait for state change
                        cond.wait().await;
                        continue;
                    }
                    // Request blocks for files
                    FsDestinationState::GetFiles { ref mut files_to_request, ref mut cond, .. } => {
                        match files_to_request.pop_front() {
                            Some(name) => {
                                if log_enabled!(Debug) {
                                    debug!("FsDestination::stream: send GetFile({:?})", String::from_utf8_lossy(&name));
                                }
                                DestinationEvent::GetFile(name)
                            }
                            None => {
                                debug!("FsDestination::stream: no more files, waiting...");
                                cond.wait().await;
                                continue;
                            }
                        }
                    }
                    // Request block data
                    FsDestinationState::GetBlocks { ref mut blocks_to_request, .. } => {
                        match blocks_to_request {
                            Some(ref mut l) => match l.pop_front() {
                                Some(hash) => {
                                    debug!("FsDestination::stream: send GetBlock({})", hash);
                                    DestinationEvent::GetBlock(hash)
                                }
                                None => {
                                    debug!("FsDestination::stream: no more blocks, send Complete");
                                    *blocks_to_request = None;
                                    DestinationEvent::Complete
                                }
                            }
                            None => {
                                debug!("FsDestination::stream: done");
                                return None;
                            }
                        }
                    }
                };
                return Some((Ok(ret), inner));
            }
        }
    }

    fn sink(inner: Rc<RefCell<FsDestinationInner>>, event: SourceEvent) -> impl Future<Output=Result<Rc<RefCell<FsDestinationInner>>, Error>> {
        async move {
            {
                let mut inner_: std::cell::RefMut<FsDestinationInner> = inner.borrow_mut();
                let inner_: &mut FsDestinationInner = inner_.deref_mut();

                // Can't mutably borrow more than once
                let mut new_state: Option<FsDestinationState> = None;
                let state = &mut inner_.state;
                let index = &mut inner_.index;
                let root_dir = &inner_.root_dir;

                debug!("FsDestination::sink: recv {:?}", event);

                match state {
                    // Receive files list
                    FsDestinationState::FilesList { ref mut cond } => {
                        match event {
                            SourceEvent::FileEntry(path, _size, blocks_hash) => {
                                let path: PathBuf = String::from_utf8(path)
                                    .expect("encoding")
                                    .into();
                                let file = inner_.index.get_file(&path)?;
                                let add = match file {
                                    Some((_file_id, _modified, recorded_blocks_hash)) => {
                                        if blocks_hash == recorded_blocks_hash {
                                            debug!("FsDestination::sink:  file's blocks_hash matches");
                                            false // File is up to date, do nothing
                                        } else {
                                            debug!("FsDestination::sink: file exists but blocks_hash differs");
                                            true
                                        }
                                    }
                                    None => {
                                        debug!("FsDestination::sink: file doesn't exist");
                                        true
                                    }
                                };
                                if add {
                                    // Create temporary file
                                    let temp_path = temp_name(&path)?;
                                    let now: chrono::DateTime<chrono::Utc> = chrono::Utc::now();
                                    inner_.index.add_file_overwrite(&temp_path, now)?;
                                    let temp_path = inner_.root_dir.join(temp_path);
                                    debug!("FsDestination::sink: creating temp file {:?}", temp_path);
                                    if let Some(parent) = temp_path.parent() {
                                        std::fs::create_dir_all(parent)?;
                                    }
                                    OpenOptions::new()
                                        .write(true)
                                        .truncate(true)
                                        .create(true)
                                        .open(temp_path)?;
                                }
                            }
                            SourceEvent::EndFiles => {
                                // FIXME: Don't get all files at once, iterate
                                let mut files_to_request = VecDeque::new();
                                for name in index.list_temp_files()? {
                                    let name = untemp_name(&name)?;
                                    let name = name
                                        .into_os_string()
                                        .into_string()
                                        .expect("encoding")
                                        .into_bytes();
                                    files_to_request.push_back(name);
                                }
                                let files_to_receive = files_to_request.len();
                                debug!("FsDestination::sink: state=GetFiles({} files)", files_to_receive);
                                new_state = Some(FsDestinationState::GetFiles {
                                    files_to_request,
                                    files_to_receive,
                                    cond: Default::default(),
                                    file_blocks_id: None,
                                });
                                cond.set();
                            }
                            _ => return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Unexpected message from source").into()),
                        }
                    }
                    // Receive blocks for files
                    FsDestinationState::GetFiles { ref mut cond, ref mut file_blocks_id, ref mut files_to_receive, .. } => {
                        match (*file_blocks_id, event) {
                            (None, SourceEvent::FileStart(path)) => {
                                let path: PathBuf = String::from_utf8(path)
                                    .expect("encoding")
                                    .into();
                                let (file_id, _modified) = index.get_temp_file(&path)?
                                    .ok_or(std::io::Error::new(std::io::ErrorKind::NotFound, format!("Unknown file {:?}", path)))?;
                                *file_blocks_id = Some((file_id, 0))
                            }
                            // FIXME: Don't need to capture all of them by ref,
                            // but necessary for Rust 1.45
                            (Some((ref file_id, ref mut offset)), SourceEvent::FileBlock(ref hash, ref size)) => {
                                // TODO: Maybe copy it right now?
                                // TODO: Is file finished? If yes, move to destination, update index
                                index.add_missing_block(&hash, *file_id, *offset, *size)?;
                                *offset += *size;
                            }
                            (Some(_), SourceEvent::FileEnd) => {
                                *file_blocks_id = None;
                                *files_to_receive -= 1;
                                debug!("FsDestination::sink: {} files left to receive", *files_to_receive);
                                if *files_to_receive == 0 {
                                    // FIXME: Don't get all files at once, iterate
                                    let mut blocks_to_request = VecDeque::new();
                                    for hash in index.list_missing_blocks()? {
                                        blocks_to_request.push_back(hash);
                                    }
                                    let blocks_to_receive = blocks_to_request.len();
                                    debug!("FsDestination::sink: state=GetBlocks({} blocks)", blocks_to_receive);
                                    new_state = Some(FsDestinationState::GetBlocks {
                                        blocks_to_request: Some(blocks_to_request),
                                        blocks_to_receive,
                                    });
                                    cond.set();
                                }
                            }
                            _ => return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Unexpected message from source").into()),
                        }
                    }
                    // Receiving block data
                    FsDestinationState::GetBlocks { ref mut blocks_to_receive, .. } => {
                        match event {
                            SourceEvent::BlockData(hash, data) => {
                                for (name, offset, _size) in index.list_block_locations(&hash)? {
                                    debug!("FsDestination::sink: writing block to {:?} offset {}", name, offset);
                                    write_block(&root_dir.join(&name), offset, &data)?;
                                    // TODO: Is file finished? If yes, move to destination, update index
                                }
                                *blocks_to_receive -= 1; // Do we need to keep track of this?
                                debug!("FsDestination::sink: {} blocks left to receive", *blocks_to_receive);
                            }
                            _ => return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Unexpected message from source").into()),
                        }
                    }
                }
                if let Some(s) = new_state {
                    *state = s;
                }
            }
            Ok(inner)
        }
    }
}
