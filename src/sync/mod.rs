//! This module contains the transfer protocol handlers.

pub mod fs;
pub mod locations;
//pub mod ssh;

use log::info;
use futures::join;
use futures::sink::Sink;
use futures::stream::{LocalBoxStream, StreamExt};
use std::pin::Pin;

use crate::{Error, HashDigest};

pub enum SourceEvent {
    FileEntry(Vec<u8>, usize, HashDigest),
    EndFiles,
    FileStart(Vec<u8>),
    FileBlock(HashDigest, usize),
    FileEnd,
    BlockData(HashDigest, Vec<u8>),
}

impl std::fmt::Debug for SourceEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            &SourceEvent::FileEntry(ref path, size, ref hash) => write!(
                f,
                "FileEntry({}, {}, {})",
                String::from_utf8_lossy(&path),
                size,
                hash,
            ),
            &SourceEvent::EndFiles => write!(f, "EndFiles"),
            &SourceEvent::FileStart(ref path) => write!(
                f,
                "FileStart({})",
                String::from_utf8_lossy(&path),
            ),
            &SourceEvent::FileBlock(ref hash, size) => write!(
                f,
                "FileBlock({}, {})",
                hash,
                size,
            ),
            &SourceEvent::FileEnd => write!(f, "FileEnd"),
            &SourceEvent::BlockData(ref hash, ref data) => write!(
                f,
                "BlockData({}, <{} bytes>)",
                hash,
                data.len(),
            ),
        }
    }
}

pub enum DestinationEvent {
    GetFile(Vec<u8>),
    GetBlock(HashDigest),
    Complete,
}

impl std::fmt::Debug for DestinationEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            &DestinationEvent::GetFile(ref path) => write!(
                f,
                "GetFile({})",
                String::from_utf8_lossy(&path),
            ),
            &DestinationEvent::GetBlock(ref hash) => write!(f, "GetBlock({})", hash),
            &DestinationEvent::Complete => write!(f, "Complete"),
        }
    }
}

/// The source, representing where the files are coming from.
///
/// This is relative to a single process, e.g. the sending side has a source
/// that reads from files, and the receiving side has a source that reads from
/// the network.
pub trait Source {
    fn streams<'a>(&'a mut self) -> (LocalBoxStream<'a, Result<SourceEvent, Error>>, Pin<Box<dyn Sink<DestinationEvent, Error=Error> + 'a>>);
}

/// The destination, representing where the files are being sent.
///
/// This is relative to a single process, e.g. the sending side has a
/// destination encapsulating some network protocol, and the receiving side has
/// a destination that actually updates files.
pub trait Destination {
    fn streams<'a>(&'a mut self) -> (LocalBoxStream<'a, Result<DestinationEvent, Error>>, Pin<Box<dyn Sink<SourceEvent, Error=Error>+ 'a>>);
}

// Generic impls for mutable references to trait objects (dynamic dispatch)

impl Source for &mut dyn Source {
    fn streams<'a>(&'a mut self) -> (LocalBoxStream<'a, Result<SourceEvent, Error>>, Pin<Box<dyn Sink<DestinationEvent, Error=Error> + 'a>>) {
        (*self).streams()
    }
}

impl Destination for &mut dyn Destination {
    fn streams<'a>(&'a mut self) -> (LocalBoxStream<'a, Result<DestinationEvent, Error>>, Pin<Box<dyn Sink<SourceEvent, Error=Error>+ 'a>>) {
        (*self).streams()
    }
}

// Generic impls for boxed trait objects (dynamic dispatch)

impl Source for Box<dyn Source> {
    fn streams<'a>(&'a mut self) -> (LocalBoxStream<'a, Result<SourceEvent, Error>>, Pin<Box<dyn Sink<DestinationEvent, Error=Error> + 'a>>) {
        let s: &'a mut dyn Source = self.as_mut();
        s.streams()
    }
}

impl Destination for Box<dyn Destination> {
    fn streams<'a>(&'a mut self) -> (LocalBoxStream<'a, Result<DestinationEvent, Error>>, Pin<Box<dyn Sink<SourceEvent, Error=Error>+ 'a>>) {
        let s: &'a mut dyn Destination = self.as_mut();
        s.streams()
    }
}

trait SynchronousSource {
    type FilesIterator: Iterator<Item=(Vec<u8>, usize, HashDigest)>;
    type FileBlocksIterator: Iterator<Item=(HashDigest, usize)>;

    fn list_files(&self) -> Self::FilesIterator;
    fn get_file_blocks(&self, name: &[u8]) -> Self::FileBlocksIterator;
    fn get_block(&self, hash: &HashDigest) -> Vec<u8>;
}

trait SynchronousDestination {
    type MissingBlocksIterator: Iterator<Item=HashDigest>;

    fn add_file(&self, name: &[u8], size: usize, hash: &HashDigest);
    fn set_file_blocks(&self, path: &[u8], hash: &HashDigest);
    fn list_missing_blocks(&self) -> Self::MissingBlocksIterator;
}

pub async fn do_sync<S: Source, R: Destination>(
    mut source: S,
    mut destination: R,
) -> Result<(), Error> {
    info!("Starting sync...");
    let (source_from, source_to) = source.streams();
    let (destination_from, destination_to) = destination.streams();
    info!("Streams opened");

    // Concurrently forward streams into sinks
    let (r1, r2) = join!(
        source_from.forward(destination_to),
        destination_from.forward(source_to),
    );
    r1?;
    r2?;

    Ok(())
}
