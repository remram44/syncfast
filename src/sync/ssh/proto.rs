use copy_in_place::copy_in_place;
use std::borrow::Cow;
use std::error::Error;
use std::fmt;
use std::ops::{Deref, Range};
use std::path::Path;

#[derive(Debug, PartialEq, Eq)]
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
                .position(|&b| { b == b' ' || b == b'\n' })
            {
                let space_idx = prev_pos + space_idx;
                let slice = self.pos .. space_idx;
                self.pos = space_idx;
                if self.buffer[space_idx] == b' ' {
                    self.pos += 1;
                }
                // Return slice
                return Ok(slice);
            } else {
                prev_pos = self.size;
            }

            // Read more bytes
            self.read()?;
        }
    }

    /// Read until the end of the line (consume the line ending too)
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
                self.pos = eol_idx + 1;
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
                .position(|&b| b == b':')
            {
                let colon_idx = prev_pos + colon_idx;
                let size: usize = {
                    // Get the size
                    let slice = &self.buffer[self.pos .. colon_idx];

                    // Parse it to a number
                    std::str::from_utf8(slice)
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .ok_or(CommunicationError::ProtocolError(
                            "Invalid string size",
                        ))?
                };

                // Read the string
                if colon_idx + 1 + size > self.size {
                    let bytes = colon_idx + 1 + size - self.size; 
                    self.read_at_least(bytes)?;
                }

                // Return slice
                self.pos = colon_idx + 1 + size;
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
                .position(|&b| b == b':')
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
                self.pos = self.size.min(colon_idx + 1 + size);
                block.extend_from_slice(
                    &self.buffer[colon_idx + 1 .. self.pos],
                );

                // Clear this from our buffer
                copy_in_place(&mut self.buffer, self.pos .. self.size, colon_idx + 1);
                self.size = colon_idx + 1 + self.size - self.pos;
                self.pos = colon_idx + 1;

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
        if self.pos + 1 > self.size {
            self.read_at_least(1)?;
        }
        if self.buffer[self.pos] != b' ' {
            return Err(CommunicationError::ProtocolError("Missing space"));
        }
        self.pos += 1;
        Ok(())
    }

    /// Consume a line ending
    pub fn read_eol(&mut self) -> Result<(), CommunicationError<E>> {
        if self.pos + 1 > self.size {
            self.read_at_least(1)?;
        }
        if self.buffer[self.pos] != b'\n' {
            return Err(CommunicationError::ProtocolError("Missing line ending"));
        }
        self.pos += 1;
        Ok(())
    }

    /// Clear what was consumed from the buffer
    pub fn end(&mut self) {
        copy_in_place(&mut self.buffer, self.pos .. self.size, 0);
        self.size -= self.pos;
        self.pos = 0;
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

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::fmt;

    use super::SyncReader;

    #[derive(Debug)]
    struct FakeEol;

    impl fmt::Display for FakeEol {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "fake end")
        }
    }

    impl std::error::Error for FakeEol {}

    fn fake_reader(
        reads: Vec<&'static str>,
    ) -> impl FnMut(&mut [u8]) -> Result<usize, FakeEol> {
        let mut reads: VecDeque<&'static str> = reads.into_iter().collect();
        move |buf| {
            match reads.pop_front() {
                None => Err(FakeEol),
                Some(v) => {
                    assert!(v.len() <= buf.len());
                    buf[.. v.len()].clone_from_slice(v.as_bytes());
                    Ok(v.len())
                }
            }
        }
    }

    #[test]
    fn test_fakereader() {
        let mut reader = SyncReader::new(fake_reader(
            vec![],
        ));
        match reader.read_str() {
            Ok(_) => panic!(),
            Err(e) => assert_eq!(format!("{}", e), "fake end"),
        }

        let mut reader = SyncReader::new(fake_reader(
            vec![""],
        ));
        match reader.read_str() {
            Ok(_) => panic!(),
            Err(e) => assert_eq!(format!("{}", e), "fake end"),
        }

        let mut reader = SyncReader::new(fake_reader(
            vec!["test", " more data"],
        ));
        assert_eq!(reader.read().unwrap(), 4);
        assert_eq!(&reader[.. 4], b"test");
        assert_eq!(reader.read().unwrap(), 10);
        assert_eq!(&reader[.. 14], b"test more data");
    }

    #[test]
    fn test_syncreader() {
        let mut reader = SyncReader::new(fake_reader(
            vec![
                "FIL", "E 1", "3:test/f", "ile.txt",
                " 1572", "807874\na",
            ],
        ));
        assert_eq!(reader.pos, 0);

        let cmd = reader.read_to_space().unwrap();
        assert_eq!(reader.pos, 5);
        assert_eq!(reader.size, 6);
        assert_eq!(&reader[cmd], b"FILE");

        let name = reader.read_str().unwrap();
        assert_eq!(reader.pos, 21);
        assert_eq!(reader.size, 21);
        assert_eq!(&reader[name], b"test/file.txt");

        reader.read_space().unwrap();
        assert_eq!(reader.pos, 22);
        assert_eq!(reader.size, 26);

        let ts = reader.read_to_eol().unwrap();
        assert_eq!(reader.pos, 33);
        assert_eq!(reader.size, 34);
        assert_eq!(&reader[ts], b"1572807874");

        reader.end();
        assert_eq!(reader.pos, 0);
        assert_eq!(reader.size, 1);
        assert_eq!(reader.buffer[0], b'a');
    }

    #[test]
    fn test_syncreader_block_once() {
        let mut reader = SyncReader::new(fake_reader(
            vec!["TEST ", "5:abcde\n"],
        ));
        assert_eq!(reader.pos, 0);

        let cmd = reader.read_to_space().unwrap();
        assert_eq!(reader.pos, 5);
        assert_eq!(reader.size, 5);
        assert_eq!(&reader[cmd], b"TEST");

        let block = reader.read_block().unwrap();
        assert_eq!(reader.pos, 12);
        assert_eq!(reader.size, 13);
        assert_eq!(&block, b"abcde");
        assert_eq!(&reader.buffer[.. 13], b"TEST 5:abcde\n")
    }

    #[test]
    fn test_syncreader_block_readagain() {
        let mut reader = SyncReader::new(fake_reader(
            vec!["TEST ", "5:abc", "de", "\n"],
        ));
        assert_eq!(reader.pos, 0);

        let cmd = reader.read_to_space().unwrap();
        assert_eq!(reader.pos, 5);
        assert_eq!(reader.size, 5);
        assert_eq!(&reader[cmd], b"TEST");

        let block = reader.read_block().unwrap();
        assert_eq!(reader.pos, 10);
        assert_eq!(reader.size, 10);
        assert_eq!(&block, b"abcde");
        assert_eq!(&reader.buffer[.. 10], b"TEST 5:abc")
    }
}
