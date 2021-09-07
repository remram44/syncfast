//! Synchronization from and to local files.

use cdchunking::{Chunker, ZPAQ};
use futures::channel::mpsc::{Receiver, channel};
use futures::sink::{Sink, SinkExt};
use futures::stream::{LocalBoxStream, Stream, StreamExt};
use log::{debug, info, warn};
use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll};

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
            FsSourceFrom {
                index: &mut self.index,
                receiver: Box::pin(receiver),
                state: FsSourceState::ListFiles(None),
            }.boxed_local(),
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
    receiver: Pin<Box<Receiver<DestinationEvent>>>,
    state: FsSourceState,
}

impl<'a> Stream for FsSourceFrom<'a> {
    type Item = Result<SourceEvent, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Result<SourceEvent, Error>>> {
        // This is safe to unpin, so do that
        let s = Pin::into_inner(self);

        match s.state {
            // Send files list
            FsSourceState::ListFiles(ref mut list) => {
                // If we don't have data, fetch from database
                if list.is_none() {
                    let files = s.index.list_files()?;
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
                    Some((path, size, blocks_hash)) => Poll::Ready(Some(Ok(SourceEvent::FileEntry(path, size, blocks_hash)))),
                    None => {
                        s.state = FsSourceState::Respond;
                        Poll::Ready(Some(Ok(SourceEvent::EndFiles)))
                    }
                }
            }
            // Files are sent, respond to requests
            FsSourceState::Respond => {
                let req = match s.receiver.as_mut().poll_next(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(None) => return Poll::Ready(None),
                    Poll::Ready(Some(e)) => e,
                };
                match req {
                    DestinationEvent::GetFile(path) => {
                        let path_str = String::from_utf8(path).expect("encoding");
                        let (file_id, _modified, _blocks_hash) = match s.index.get_file(Path::new(&path_str))? {
                            Some(t) => t,
                            None => return Poll::Ready(Some(Err(Error::Io(std::io::Error::new(std::io::ErrorKind::NotFound, "Requested file is unknown"))))),
                        };
                        let blocks = s.index.list_file_blocks(file_id)?;
                        let mut new_blocks = VecDeque::with_capacity(blocks.len());
                        for (hash, _offset, size) in blocks {
                            new_blocks.push_back((hash, size));
                        }
                        s.state = FsSourceState::ListBlocks(new_blocks);
                        Poll::Ready(Some(Ok(SourceEvent::FileStart(path_str.into_bytes()))))
                    }
                    DestinationEvent::GetBlock(hash) => {
                        let (path, offset, _size) = match s.index.get_block(&hash)? {
                            Some(t) => t,
                            None => return Poll::Ready(Some(Err(Error::Io(std::io::Error::new(std::io::ErrorKind::NotFound, "Requested block is unknown"))))),
                        };
                        let data = read_block(&path, offset)?;
                        Poll::Ready(Some(Ok(SourceEvent::BlockData(data))))
                    }
                    DestinationEvent::Complete => {
                        s.state = FsSourceState::Done;
                        Poll::Ready(None)
                    }
                }
            }
            // List blocks
            FsSourceState::ListBlocks(ref mut list) => {
                match list.pop_front() {
                    Some((hash, size)) => Poll::Ready(Some(Ok(SourceEvent::FileBlock(hash, size)))),
                    None => {
                        s.state = FsSourceState::Respond;
                        Poll::Ready(Some(Ok(SourceEvent::FileEnd)))
                    }
                }
            }
            // Stream is done
            FsSourceState::Done => Poll::Ready(None),
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
