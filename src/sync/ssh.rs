use std::path::Path;

use crate::{Error, HashDigest};
use crate::locations::SshLocation;
use crate::sync::{IndexEvent, Sink, SinkWrapper, Source, SourceWrapper};

pub struct SshSink<'a> {
    location: &'a SshLocation,
}

impl<'a> Sink for SshSink<'a> {
    // TODO: Implement SshSink
    fn new_file(
        &mut self,
        name: &Path,
        modified: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), Error>
    {
        unimplemented!()
    }

    fn new_block(
        &mut self,
        hash: &HashDigest,
        size: usize,
    ) -> Result<(), Error>
    {
        unimplemented!()
    }

    fn end_files(&mut self) -> Result<(), Error> {
        unimplemented!()
    }

    fn feed_block(
        &mut self,
        hash: &HashDigest,
        block: &[u8],
    ) -> Result<(), Error>
    {
        unimplemented!()
    }

    fn next_requested_block(
        &mut self,
    ) -> Result<Option<HashDigest>, Error>
    {
        unimplemented!()
    }

    fn is_missing_blocks(&self) -> Result<bool, Error> {
        unimplemented!()
    }
}

pub struct SshSource<'a> {
    location: &'a SshLocation,
}

impl<'a> Source for SshSource<'a> {
    // TODO: Implement SshSource
    fn next_from_index(&mut self) -> Result<Option<IndexEvent>, Error> {
        unimplemented!()
    }

    fn request_block(&mut self, hash: &HashDigest) -> Result<(), Error> {
        unimplemented!()
    }

    fn get_next_block(&mut self) -> Result<Option<(HashDigest, Vec<u8>)>, Error> {
        unimplemented!()
    }
}

pub struct SshWrapper(pub SshLocation);

impl SinkWrapper for SshWrapper {
    fn open<'a>(&'a mut self) -> Result<Box<dyn Sink + 'a>, Error> {
        Ok(Box::new(SshSink {
            location: &self.0,
        }))
    }
}

impl SourceWrapper for SshWrapper {
    fn open<'a>(&'a mut self) -> Result<Box<dyn Source + 'a>, Error> {
        Ok(Box::new(SshSource {
            location: &self.0,
        }))
    }
}
