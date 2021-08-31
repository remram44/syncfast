//! This module contains the transfer protocol handlers.

//pub mod fs;
pub mod locations;
//pub mod ssh;

use futures::{FutureExt, select};
use futures::future::Either;
use std::future::{Future, Pending, Ready, pending, ready};
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

/// The destination representing where the files are being sent.
///
/// This is relative to a single process, e.g. the sending side has a
/// destination encapsulating some network protocol, and the receiving side has
/// a destination that actually updates files.
pub trait Destination {
    fn receive(&mut self) -> Result<Option<DestinationEvent>, Error>;
    fn send(&mut self, event: SourceEvent) -> Result<(), Error>;
}

/// The source, representing where the files are coming from.
///
/// This is relative to a single process, e.g. the sending side has a source
/// that reads from files, and the receiving side has a source that reads from
/// the network.
pub trait Source {
    fn receive(&mut self) -> Result<Option<SourceEvent>, Error>;
    fn send(&mut self, event: DestinationEvent) -> Result<(), Error>;
}

pub trait AsyncDestination {
    type RFuture: Future<Output=Result<DestinationEvent, Error>> + Send;
    type SFuture: Future<Output=Result<(), Error>> + Send;

    fn receive(&mut self) -> Self::RFuture;
    fn send(&mut self, event: SourceEvent) -> Self::SFuture;
}

pub struct AsyncDestinationAdapter<S: Destination>(S);

impl<S: Destination> AsyncDestination for AsyncDestinationAdapter<S> {
    type RFuture = Either<Ready<Result<DestinationEvent, Error>>, Pending<Result<DestinationEvent, Error>>>;
    type SFuture = Ready<Result<(), Error>>;

    fn receive(&mut self) -> Self::RFuture {
        match self.0.receive() {
            Ok(Some(e)) => Either::Left(ready(Ok(e))),
            Ok(None) => Either::Right(pending()),
            Err(e) => Either::Left(ready(Err(e))),
        }
    }

    fn send(&mut self, event: SourceEvent) -> Self::SFuture {
        ready(self.0.send(event))
    }
}

pub trait AsyncSource<'a> {
    type RFuture: Future<Output=Result<SourceEvent, Error>> + Send + 'a;
    type SFuture: Future<Output=Result<(), Error>> + Send;

    fn receive<'b>(&'b mut self) -> Self::RFuture where 'b: 'a;
    fn send(&mut self, event: DestinationEvent) -> Self::SFuture;
}

pub struct AsyncSourceAdapter<S: Source>(S);

impl<'a, S: Source> AsyncSource<'a> for AsyncSourceAdapter<S> {
    type RFuture = Either<Ready<Result<SourceEvent, Error>>, Pending<Result<SourceEvent, Error>>>;
    type SFuture = Ready<Result<(), Error>>;

    fn receive<'b>(&'b mut self) -> Self::RFuture where 'b: 'a {
        match self.0.receive() {
            Ok(Some(e)) => Either::Left(ready(Ok(e))),
            Ok(None) => Either::Right(pending()),
            Err(e) => Either::Left(ready(Err(e))),
        }
    }

    fn send(&mut self, event: DestinationEvent) -> Self::SFuture {
        ready(self.0.send(event))
    }
}

trait BoxedAsyncSource<'a> {
    fn receive(&'a mut self) -> Pin<Box<dyn Future<Output=Result<SourceEvent, Error>> + Send + 'a>>;
}

impl<'a, S: AsyncSource<'a>> BoxedAsyncSource<'a> for S {
    fn receive(&'a mut self) -> Pin<Box<dyn Future<Output=Result<SourceEvent, Error>> + Send + 'a>> {
        (*self).receive().boxed()
    }
}

impl<'a> AsyncSource<'a> for Box<dyn BoxedAsyncSource<'a>> {
    type RFuture = Pin<Box<dyn Future<Output=Result<SourceEvent, Error>> + Send + 'a>>;
    type SFuture = Pin<Box<dyn Future<Output=Result<(), Error>> + Send + 'a>>;

    fn receive<'b>(&'b mut self) -> Self::RFuture where 'b: 'a {
        BoxedAsyncSource::receive(&mut *self)
    }

    fn send(&mut self, event: DestinationEvent) -> Self::SFuture {
        todo!()
    }
}

pub async fn do_sync<
    'a,
    SR: Future<Output=Result<SourceEvent, Error>>,
    SS: Future<Output=Result<(), Error>>,
    RR: Future<Output=Result<DestinationEvent, Error>>,
    RS: Future<Output=Result<(), Error>>,
    S: AsyncSource<'a, RFuture=SR, SFuture=SS> + 'a,
    R: AsyncDestination<RFuture=RR, SFuture=RS>,
>(
    mut source: S,
    mut destination: R,
) -> Result<(), Error> {
    let mut done = false;
    while !done {
        select! {
            msg = source.receive().fuse() => destination.send(msg?).await?,
            msg = destination.receive().fuse() => {
                let msg = msg?;
                if let DestinationEvent::Complete = msg {
                    done = true
                }
                source.send(msg).await?
            }
        };
    }
    Ok(())
}
