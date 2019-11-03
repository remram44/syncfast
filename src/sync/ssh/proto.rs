use std::borrow::Cow;
use std::error::Error;
use std::fmt;
use std::ops::{Deref, Range};
use std::path::Path;

#[derive(Debug)]
pub enum CommunicationError<E: Error> {
    ProtocolError(&'static str),
    Io(E),
}

impl<E: Error> fmt::Display for CommunicationError<E> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CommunicationError::ProtocolError(s) => write!(f, "{}", s),
            CommunicationError::Io(e) => write!(f, "{}", e),
        }
    }
}

impl<E: Error> From<E> for CommunicationError<E> {
    fn from(e: E) -> CommunicationError<E> {
        CommunicationError::Io(e)
    }
}

impl<E: Error> Error for CommunicationError<E> {}

#[cfg(unix)]
pub fn path_to_u8(path: &Path) -> Cow<[u8]> {
    use std::os::unix::ffi::OsStrExt;
    Cow::Borrowed(path.as_os_str().as_bytes())
}

#[cfg(not(unix))]
pub fn path_to_u8(path: &Path) -> Cow<[u8]> {
    match path.as_os_str().to_string_lossy() {
        Cow::Borrowed(s) => Cow::Borrowed(s.as_bytes()),
        Cow::Owned(s) => Cow::Owned(s.into_bytes()),
    }
}

#[cfg(unix)]
pub fn path_from_u8(path: &[u8]) -> Cow<Path> {
    use std::os::unix::ffi::OsStrExt;
    let osstr: &std::ffi::OsStr = OsStrExt::from_bytes(path);
    Cow::Borrowed(Path::new(osstr))
}

#[cfg(not(unix))]
pub fn path_from_u8(path: &[u8]) -> Cow<Path> {
    match String::from_utf8_lossy(path) {
        Cow::Borrowed(s) => Cow::Borrowed(Path::new(s)),
        Cow::Owned(s) => Cow::Owned(s.into()),
    }
}

pub struct SyncReader<E, F>
    where E: Error, F: FnMut(&mut [u8]) -> Result<usize, E>
{
    /// Wrapped reader
    reader: F,
    buffer: [u8; 4096],
    /// How much we have consumed of the buffer
    pos: usize,
    /// How many bytes we read to the buffer
    size: usize,
}

impl<E, F> SyncReader<E, F>
    where E: Error, F: FnMut(&mut [u8]) -> Result<usize, E>
{
    pub fn new(reader: F) -> SyncReader<E, F> {
        SyncReader { reader, buffer: [0u8; 4096], pos: 0, size: 0 }
    }

    /// Read some more bytes
    fn read(&mut self) -> Result<usize, CommunicationError<E>> {
        let bytes = (self.reader)(&mut self.buffer[self.size ..])?;
        self.size += bytes;
        Ok(bytes)
    }

    /// Read more bytes we need
    fn read_at_least(
        &mut self,
        bytes: usize,
    ) -> Result<(), CommunicationError<E>> {
        let target = self.size + bytes;
        if target > 4096 {
            return Err(CommunicationError::ProtocolError("Command too long"));
        }
        while self.size < target {
            self.read()?;
        }
        Ok(())
    }

    /// Read until the next space (consume the space too)
    pub fn read_to_space(
        &mut self,
    ) -> Result<Range<usize>, CommunicationError<E>> {
        let mut prev_pos = self.pos; // No space until here
        loop {
            // Find a space
            if let Some(space_idx) = self.buffer[prev_pos .. self.size]
                .iter()
                .position(|&b| b == b' ')
            {
                let space_idx = prev_pos + space_idx;
                let slice = self.pos .. space_idx;
                self.pos = space_idx + 1;
                // Return slice
                return Ok(slice);
            } else {
                prev_pos = self.size;
            }

            // Read more bytes
            self.read()?;
        }
    }

    /// Read until the end of the line (doesn't consume newline)
    pub fn read_to_eol(
        &mut self,
    ) -> Result<Range<usize>, CommunicationError<E>> {
        let mut prev_pos = self.pos; // No newline until here
        loop {
            // Find a newline
            if let Some(eol_idx) = self.buffer[prev_pos .. self.size]
                .iter()
                .position(|&b| b == b'\n')
            {
                let eol_idx = prev_pos + eol_idx;
                let slice = self.pos .. eol_idx;
                self.pos = eol_idx;
                // Return slice
                return Ok(slice);
            } else {
                prev_pos = self.size;
            }

            // Read more bytes
            self.read()?;
        }
    }

    /// Read a string prefixed by its length and a colon
    pub fn read_str(&mut self) -> Result<Range<usize>, CommunicationError<E>> {
        let mut prev_pos = self.pos; // No colon until here
        loop {
            // Find a colon
            if let Some(colon_idx) = self.buffer[prev_pos .. self.size]
                .iter()
                .position(|&b| b == b' ')
            {
                // Get the size
                let colon_idx = prev_pos + colon_idx;
                let size = &self.buffer[self.pos .. colon_idx];

                // Parse it to a number
                let size: usize = std::str::from_utf8(size)
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .ok_or(CommunicationError::ProtocolError(
                        "Invalid string size",
                    ))?;

                // Read the string
                if colon_idx + 1 + size > self.size {
                    self.read_at_least(colon_idx + 1 + size - self.size)?;
                }

                // Return slice
                return Ok(colon_idx + 1 .. colon_idx + 1 + size);
            } else {
                prev_pos = self.size;
            }

            // Read more bytes
            self.read()?;
        }
    }

    /// Read a long string to a vec
    pub fn read_block(&mut self) -> Result<Vec<u8>, CommunicationError<E>> {
        let mut prev_pos = self.pos; // No colon until here
        loop {
            // Find a colon
            if let Some(colon_idx) = self.buffer[prev_pos .. self.size]
                .iter()
                .position(|&b| b == b' ')
            {
                // Get the size
                let colon_idx = prev_pos + colon_idx;
                let size = &self.buffer[self.pos .. colon_idx];

                // Parse it to a number
                let size: usize = std::str::from_utf8(size)
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .ok_or(CommunicationError::ProtocolError(
                        "Invalid string size",
                    ))?;

                // Copy from buffer to vec
                let mut block = Vec::with_capacity(size);
                block.extend_from_slice(
                    &self.buffer[colon_idx + 1 .. self.size],
                );

                // Read more bytes directly into buffer
                let mut len = block.len();
                block.resize(size, 0);
                while len < size {
                    len += (self.reader)(&mut block[len .. ])?;
                }

                return Ok(block);
            } else {
                prev_pos = self.size;
            }

            // Read more bytes
            self.read()?;
        }
    }

    /// Consume a space
    pub fn read_space(&mut self) -> Result<(), CommunicationError<E>> {
        if self.pos + 1 <= self.size {
            self.read_at_least(1)?;
        }
        if self.buffer[self.pos] != b' ' {
            return Err(CommunicationError::ProtocolError("Missing space"));
        }
        self.pos += 1;
        Ok(())
    }

    /// Consume a line ending and clear what was consumed from the buffer
    pub fn end(&mut self) -> Result<(), CommunicationError<E>> {
        // Line ending
        if self.pos + 1 <= self.size {
            self.read_at_least(1)?;
        }
        if self.buffer[self.pos] != b'\n' {
            return Err(CommunicationError::ProtocolError(
                "Missing line ending",
            ));
        }
        self.pos += 1;

        // Discard what was consumed
        self.buffer.copy_within(self.pos .. self.size, 0);
        self.size -= self.pos;
        self.pos = 0;
        Ok(())
    }
}

impl<E, F> Deref for SyncReader<E, F>
    where E: Error, F: FnMut(&mut [u8]) -> Result<usize, E>
{
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.buffer[0 .. self.size]
    }
}
