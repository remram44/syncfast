//! This module contains the transfer protocol handlers.
//!
//! The general architecture is as follows:
//!
//! ```plain
//! +--------+   new index   +-------------+
//! |        | +-----------> |             |
//! | Source |               | Destination |
//! |        | request block |             |
//! |        | <-----------+ |             |
//! |        |               |             |
//! |        |  send block   |             |
//! |        | +-----------> |             |
//! +--------+               +-------------+
//! ```
//!
//! First the old index is computed and loaded in full.
//!
//! Then, the new index is fed in either all at once or in a streaming fashion.
//!
//! The destination will request blocks that are missing, which are fed in as
//! they are received.

pub mod fs;
pub mod locations;

use std::path::{Path, PathBuf};

use crate::{Error, HashDigest};
use crate::index::Index;

/// The destination representing where the files are being sent.
///
/// This is relative to a single process, e.g. the sending side has a
/// destination encapsulating some network protocol, and the receiving side has
/// a destination that actually updates files.
pub trait Destination {
    /// Start on a new file
    fn new_file(
        &mut self,
        path: &Path,
        modified: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), Error>;

    /// Feed entry from the new index
    fn new_block(
        &mut self,
        hash: &HashDigest,
        size: usize,
    ) -> Result<(), Error>;

    /// End of files
    fn end_files(&mut self) -> Result<(), Error>;

    /// Feed a block that was requested
    fn feed_block(
        &mut self,
        hash: &HashDigest,
        block: &[u8],
    ) -> Result<(), Error>;

    /// Ask which blocks to get next
    fn next_requested_block(&mut self) -> Result<Option<HashDigest>, Error>;

    /// Are we waiting on blocks?
    fn is_missing_blocks(&self) -> Result<bool, Error>;
}

/// Events that are received from the index data.
#[derive(Debug)]
pub enum IndexEvent {
    /// Start a new file (e.g. next `NewBlock` are blocks of that file)
    NewFile(PathBuf, chrono::DateTime<chrono::Utc>),

    /// Add a new block to the current file
    NewBlock(HashDigest, usize),

    /// End of the whole transfer
    End,
}

/// The source, representing where the files are coming from.
///
/// This is relative to a single process, e.g. the sending side has a source
/// that reads from files, and the receiving side has a source that reads from
/// the network.
pub trait Source {
    /// Get the next event from the index data
    fn next_from_index(&mut self) -> Result<Option<IndexEvent>, Error>;

    /// Asynchronously request a block from this source
    fn request_block(&mut self, hash: &HashDigest) -> Result<(), Error>;

    /// Get a block that was previously requested
    fn get_next_block(
        &mut self,
    ) -> Result<Option<(HashDigest, Vec<u8>)>, Error>;
}

/// Additional methods for `Destination`, through an auto-implemented trait
pub trait DestinationExt {
    /// Feed a whole new index
    fn new_index(&mut self, index: &Index) -> Result<(), Error>;
}

impl<R: Destination> DestinationExt for R {
    fn new_index(&mut self, index: &Index) -> Result<(), Error> {
        for (file_id, path, modified) in index.list_files()? {
            self.new_file(&path, modified)?;
            for (hash, _offset, size) in index.list_file_blocks(file_id)? {
                self.new_block(&hash, size)?;
            }
        }
        self.end_files()?;
        Ok(())
    }
}

impl<R: Destination + ?Sized> Destination for Box<R> {
    fn new_file(
        &mut self,
        path: &Path,
        modified: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), Error> {
        (**self).new_file(path, modified)
    }

    fn new_block(
        &mut self,
        hash: &HashDigest,
        size: usize,
    ) -> Result<(), Error> {
        (**self).new_block(hash, size)
    }

    fn end_files(&mut self) -> Result<(), Error> {
        (**self).end_files()
    }

    fn feed_block(
        &mut self,
        hash: &HashDigest,
        block: &[u8],
    ) -> Result<(), Error> {
        (**self).feed_block(hash, block)
    }

    fn next_requested_block(&mut self) -> Result<Option<HashDigest>, Error> {
        (**self).next_requested_block()
    }

    fn is_missing_blocks(&self) -> Result<bool, Error> {
        (**self).is_missing_blocks()
    }
}

impl<S: Source + ?Sized> Source for Box<S> {
    fn next_from_index(&mut self) -> Result<Option<IndexEvent>, Error> {
        (**self).next_from_index()
    }

    fn request_block(&mut self, hash: &HashDigest) -> Result<(), Error> {
        (**self).request_block(hash)
    }

    fn get_next_block(
        &mut self,
    ) -> Result<Option<(HashDigest, Vec<u8>)>, Error> {
        (**self).get_next_block()
    }
}

/// Sync from the source to the destination.
///
/// This takes care of sending instructions and blocks, and the missing block
/// requests backwards.
pub fn do_sync<S: Source, R: Destination>(
    mut source: S,
    mut destination: R,
) -> Result<(), Error> {
    let mut instructions = true;
    while instructions || destination.is_missing_blocks()? {
        // Things are done in order so that bandwidth is used in a smart way
        // For example, if you block on sending block data, you will have
        // received more block requests in the next loop, and you'll only
        // transmit (sender side) or process (receiver side) index instructions
        // when there's nothing better to do
        if let Some(hash) = destination.next_requested_block()? {
            // Block requests
            source.request_block(&hash)?; // can block on HTTP receiver side
        } else if let Some((hash, block)) = source.get_next_block()?
        // blocks on receiver side
        {
            // Block data
            destination.feed_block(&hash, &block)?; // blocks on sender side
        } else if let Some(event) = source.next_from_index()? {
            // Index instructions
            match event {
                IndexEvent::NewFile(path, modified) => {
                    destination.new_file(&path, modified)?
                }
                IndexEvent::NewBlock(hash, size) => {
                    destination.new_block(&hash, size)?
                }
                IndexEvent::End => {
                    destination.end_files()?;
                    instructions = false;
                }
            }
        }
    }
    Ok(())
}
