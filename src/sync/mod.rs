//! This module contains the transfer protocol handlers.

//pub mod fs;
pub mod locations;
//pub mod ssh;

use futures::join;
use futures::{Sink, Stream, StreamExt};
use std::pin::Pin;

use crate::{Error, HashDigest};

#[derive(Debug, PartialEq)]
pub enum Message<'a> {
    FileEntry(&'a [u8], usize, HashDigest),
    EndFiles,
    GetFile(&'a [u8]),
    FileStart(&'a [u8]),
    FileBlock(HashDigest, usize),
    FileEnd,
    GetBlock(HashDigest),
    BlockData(&'a [u8]),
    Complete,
}

#[derive(Debug, PartialEq)]
pub enum OwnedMessage {
    FileEntry(Vec<u8>, usize, HashDigest),
    EndFiles,
    GetFile(Vec<u8>),
    FileStart(Vec<u8>),
    FileBlock(HashDigest, usize),
    FileEnd,
    GetBlock(HashDigest),
    BlockData(Vec<u8>),
    Complete,
}

impl<'a> From<Message<'a>> for OwnedMessage {
    fn from(msg: Message<'a>) -> OwnedMessage {
        match msg {
            Message::FileEntry(name, size, digest) => OwnedMessage::FileEntry(name.to_owned(), size, digest),
            Message::EndFiles => OwnedMessage::EndFiles,
            Message::GetFile(name) => OwnedMessage::GetFile(name.to_owned()),
            Message::FileStart(name) => OwnedMessage::FileStart(name.to_owned()),
            Message::FileBlock(digest, size) => OwnedMessage::FileBlock(digest, size),
            Message::FileEnd => OwnedMessage::FileEnd,
            Message::GetBlock(digest) => OwnedMessage::GetBlock(digest),
            Message::BlockData(data) => OwnedMessage::BlockData(data.to_owned()),
            Message::Complete => OwnedMessage::Complete,
        }
    }
}

impl<'a> From<&'a OwnedMessage> for Message<'a> {
    fn from(msg: &'a OwnedMessage) -> Message<'a> {
        match msg {
            &OwnedMessage::FileEntry(ref name, size, ref digest) => Message::FileEntry(name, size, digest.clone()),
            &OwnedMessage::EndFiles => Message::EndFiles,
            &OwnedMessage::GetFile(ref name) => Message::GetFile(name),
            &OwnedMessage::FileStart(ref name) => Message::FileStart(name),
            &OwnedMessage::FileBlock(ref digest, size) => Message::FileBlock(digest.clone(), size),
            &OwnedMessage::FileEnd => Message::FileEnd,
            &OwnedMessage::GetBlock(ref digest) => Message::GetBlock(digest.clone()),
            &OwnedMessage::BlockData(ref data) => Message::BlockData(data),
            &OwnedMessage::Complete => Message::Complete,
        }
    }
}

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
