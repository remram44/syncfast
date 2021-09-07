//! This module contains the transfer protocol handlers.

//pub mod fs;
pub mod locations;
//pub mod ssh;

use futures::join;
use futures::{Sink, Stream, StreamExt};
use std::pin::Pin;

use crate::{Error, HashDigest};

pub enum SourceEvent {
    FileEntry(Vec<u8>, usize, HashDigest),
    EndFiles,
    FileStart(Vec<u8>),
    FileBlock(HashDigest, usize),
    FileEnd,
    BlockData(Vec<u8>),
}

pub enum DestinationEvent {
    GetFile(Vec<u8>),
    GetBlock(HashDigest),
    Complete,
}

/// The source, representing where the files are coming from.
///
/// This is relative to a single process, e.g. the sending side has a source
/// that reads from files, and the receiving side has a source that reads from
/// the network.
pub trait Source<'a> {
    type From: Stream<Item=SourceEvent> + Send + 'a;
    type To: Sink<DestinationEvent, Error=Error> + Send + 'a;

    fn streams(&mut self) -> (Self::From, Self::To);
}

/// The destination, representing where the files are being sent.
///
/// This is relative to a single process, e.g. the sending side has a
/// destination encapsulating some network protocol, and the receiving side has
/// a destination that actually updates files.
pub trait Destination<'a> {
    type From: Stream<Item=DestinationEvent> + Send + 'a;
    type To: Sink<SourceEvent, Error=Error> + Send + 'a;

    fn streams(&mut self) -> (Self::From, Self::To);
}

type BoxDestination<'a> = Box<dyn Destination<'a, From=Pin<Box<dyn Stream<Item=SourceEvent> + Send + 'a>>, To=Pin<Box<dyn Sink<DestinationEvent, Error=Error> + Send + 'a>>>>;

type BoxSource<'a> = Box<dyn Source<'a, From=Pin<Box<dyn Stream<Item=SourceEvent> + Send + 'a>>, To=Pin<Box<dyn Sink<DestinationEvent, Error=Error> + Send + 'a>>>>;

impl<'a, S: Source<'a> + Send + 'a> Source<'a> for Box<S> {
    type From = Pin<Box<dyn Stream<Item=SourceEvent> + Send + 'a>>;
    type To = Pin<Box<dyn Sink<DestinationEvent, Error=Error> + Send + 'a>>;

    fn streams(&mut self) -> (Self::From, Self::To) {
        let (s_from, s_to) = Source::streams(&mut **self);
        (s_from.boxed(), Box::pin(s_to))
    }
}

impl<'a, S: Destination<'a> + Send + 'a> Destination<'a> for Box<S> {
    type From = Pin<Box<dyn Stream<Item=DestinationEvent> + Send + 'a>>;
    type To = Pin<Box<dyn Sink<SourceEvent, Error=Error> + Send + 'a>>;

    fn streams(&mut self) -> (Self::From, Self::To) {
        let (s_from, s_to) = Destination::streams(&mut **self);
        (s_from.boxed(), Box::pin(s_to))
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

pub async fn do_sync<'a, S: Source<'a> + 'a, R: Destination<'a> + 'a>(
    mut source: S,
    mut destination: R,
) -> Result<(), Error> {
    let (source_from, source_to) = source.streams();
    let (destination_from, destination_to) = destination.streams();

    // Concurrently forward streams into sinks
    let (r1, r2) = join!(
        source_from.map(|v| Ok(v)).forward(destination_to),
        destination_from.map(|v| Ok(v)).forward(source_to),
    );
    r1?;
    r2?;

    Ok(())
}
