//! This module contains the transfer protocol handlers.
//!
//! The general architecture is as follows:
//!
//! ```plain
//! +--------+   new index   +------+
//! |        | +-----------> |      |
//! | Source |               | Sink |
//! |        | request block |      |
//! |        | <-----------+ |      |
//! |        |               |      |
//! |        |  send block   |      |
//! |        | +-----------> |      |
//! +--------+               +------+
//! ```
//!
//! First the old index is computed and loaded in full.
//!
//! Then, the new index is fed in either all at once or in a streaming fashion.
//!
//! The sink will request blocks that are missing from the destination,
//! which are fed in as they are received.

pub mod fs;
pub mod ssh;

use std::path::PathBuf;

use crate::{Error, HashDigest};

/// Events from the sink.
pub enum SinkEvent {
    /// A block is needed
    BlockRequest(HashDigest),

    /// The sink does not need further blocks
    End,
}

/// Events from the source.
pub enum SourceEvent {
    /// Start a new file (e.g. next `NewBlock` are blocks of that file)
    NewFile(PathBuf, chrono::DateTime<chrono::Utc>),

    /// Add a new block to the current file
    NewBlock(HashDigest, usize),

    /// No more instructions
    End,

    /// Data for a block that was requested
    BlockData(HashDigest, Vec<u8>),
}

/// The sink, representing where the files are being sent.
///
/// This is relative to a single process, e.g. the sending side has a sink
/// encapsulating some network protocol, and the receiving side has a sink that
/// actually updates files.
pub trait Sink {
    fn next_event(&mut self) -> Result<Option<SinkEvent>, Error>;

    fn feed_event(&mut self, SourceEvent) -> Result<(), Error>;

    /// Are we waiting on blocks?
    fn is_missing_blocks(&self) -> Result<bool, Error>;
}

/// The source, representing where the files are coming from.
///
/// This is relative to a single process, e.g. the sending side has a source
/// that reads from files, and the receiving side has a source that reads from
/// the network.
pub trait Source {
    fn next_event(&mut self) -> Result<Option<SourceEvent>, Error>;

    fn feed_event(&mut self, SinkEvent) -> Result<(), Error>;
}

impl<R: Sink + ?Sized> Sink for Box<R> {
    fn next_event(&mut self) -> Result<Option<SinkEvent>, Error> {
        (**self).next_event()
    }

    fn feed_event(&mut self, event: SourceEvent) -> Result<(), Error> {
        (**self).feed_event(event)
    }

    fn is_missing_blocks(&self) -> Result<bool, Error> {
        (**self).is_missing_blocks()
    }
}

impl<S: Source + ?Sized> Source for Box<S> {
    fn next_event(&mut self) -> Result<Option<SourceEvent>, Error> {
        (**self).next_event()
    }

    fn feed_event(&mut self, event: SinkEvent) -> Result<(), Error> {
        (**self).feed_event(event)
    }
}

/// Wrapper for ownership reasons
///
/// This only exists to hold the rusqlite `Connection`, which has to outlive
/// the transaction used by the `Sink`.
pub trait SinkWrapper {
    fn open<'a>(&'a mut self) -> Result<Box<dyn Sink + 'a>, Error>;
}

impl<RW: SinkWrapper + ?Sized> SinkWrapper for Box<RW> {
    fn open<'a>(&'a mut self) -> Result<Box<dyn Sink + 'a>, Error> {
        (**self).open()
    }
}

/// Wrapper for ownership reasons
///
/// This only exists to hold the rusqlite `Connection`, which has to outlive
/// the transaction used by the `Source`.
pub trait SourceWrapper {
    fn open<'a>(&'a mut self) -> Result<Box<dyn Source + 'a>, Error>;
}

impl<SW: SourceWrapper + ?Sized> SourceWrapper for Box<SW> {
    fn open<'a>(&'a mut self) -> Result<Box<dyn Source + 'a>, Error> {
        (**self).open()
    }
}

/// Sync from the source to the sink.
///
/// This takes care of sending instructions and blocks, and the missing block
/// requests backwards.
pub fn do_sync<S: Source, R: Sink>(
    mut source: S,
    mut sink: R,
) -> Result<(), Error> {
    let mut instructions = true;

    while instructions || sink.is_missing_blocks()? {
        info!("pumping");

        {
            if let Some(event) = source.next_event()? {
                if let SourceEvent::End = event {
                    instructions = false;
                }
                sink.feed_event(event)?;
            }
        }

        {
            if let Some(event) = sink.next_event()? {
                if !instructions {
                    if let SinkEvent::End = event {
                        break;
                    }
                }
                source.feed_event(event)?;
            }
        }
    }

    info!("over");
    Ok(())
}
