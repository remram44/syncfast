//! This module contains the transfer protocol handlers.

pub mod fs;
pub mod locations;
mod utils;

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
pub struct Source {
    stream: LocalBoxStream<'static, Result<SourceEvent, Error>>,
    sink: Pin<Box<dyn Sink<DestinationEvent, Error=Error>>>,
}

/// The destination, representing where the files are being sent.
///
/// This is relative to a single process, e.g. the sending side has a
/// destination encapsulating some network protocol, and the receiving side has
/// a destination that actually updates files.
pub struct Destination {
    stream: LocalBoxStream<'static, Result<DestinationEvent, Error>>,
    sink: Pin<Box<dyn Sink<SourceEvent, Error=Error>>>,
}

pub async fn do_sync(
    source: Source,
    destination: Destination,
) -> Result<(), Error> {
    info!("Starting sync...");
    let Source { stream: source_from, sink: source_to } = source;
    let Destination { stream: destination_from, sink: destination_to } = destination;
    info!("Streams opened");

    // Concurrently forward streams into sinks
    let (r1, r2) = join!(
        source_from.forward(destination_to),
        destination_from.forward(source_to),
    );
    r1?;
    r2?;
    info!("Sync complete");

    Ok(())
}
