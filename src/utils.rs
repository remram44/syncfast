use std::cmp::min;
use std::io::{self, Read, Write};

pub trait ReadExt: Read {
    /// Wrapper for `Read::read()` that retries when interrupted
    ///
    /// This will never fail with `ErrorKind::Interrupted`, and will only
    /// return less that the requested size if the end-of-file has been
    /// reached.
    /// The downside is that if an error is returned, an unknown number of
    /// bytes might have been read.
    fn read_retry(&mut self, buffer: &mut [u8]) -> io::Result<usize>;

    /// Reads an exact size into a buffer or fail.
    fn read_exact_(&mut self, buffer: &mut [u8]) -> io::Result<()>;

    /// Asserts that you reached the end of the file.
    fn read_eof(&mut self) -> io::Result<()>;
}

impl<R: Read> ReadExt for R {
    fn read_retry(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let mut read = 0;
        while read < buffer.len() {
            match self.read(&mut buffer[read..]) {
                Err(e) => {
                    if e.kind() == io::ErrorKind::Interrupted {
                        continue;
                    } else {
                        return Err(e);
                    }
                }
                Ok(0) => {
                    break;
                }
                Ok(n) => {
                    read += n;
                    continue;
                }
            }
        }
        Ok(read)
    }

    fn read_exact_(&mut self, buffer: &mut [u8]) -> io::Result<()> {
        if try!(self.read_retry(buffer)) != buffer.len() {
            return Err(io::Error::new(io::ErrorKind::InvalidData,
                                      "Unexpected end of file"));
        }
        Ok(())
    }

    fn read_eof(&mut self) -> io::Result<()> {
        if try!(self.read(&mut [0u8])) != 0 {
            return Err(io::Error::new(io::ErrorKind::InvalidData,
                                      "Trailing data at end of file"));
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub enum CopyMode {
    Exact(usize),
    Maximum(usize),
    All,
}

pub fn copy<R: Read, W: Write>(reader: &mut R, writer: &mut W,
                               what: CopyMode)
    -> io::Result<usize>
{
    let mut buffer: [u8; 4096] = unsafe { ::std::mem::uninitialized() };
    let mut copied = 0;
    while match what {
        CopyMode::All => true,
        CopyMode::Exact(len) | CopyMode::Maximum(len) => copied < len,
    } {
        let len = match what {
            CopyMode::Exact(len) | CopyMode::Maximum(len) => {
                min(4096, len - copied)
            }
            CopyMode::All => 4096,
        };
        let read = try!(reader.read(&mut buffer[..len]));
        if read > 0 {
            try!(writer.write_all(&buffer[..read]));
            copied += read;
        }
        if read != len {
            if let CopyMode::Exact(_) = what {
                return Err(io::Error::new(io::ErrorKind::Other,
                                          "Unexpected end of file"));
            } else {
                break;
            }
        }
    }
    Ok(copied)
}

pub fn to_hex(sha1: &[u8]) -> String {
    let mut hexdigest = Vec::with_capacity(40);
    for sha1byte in sha1.iter() {
        write!(hexdigest, "{:02X}", sha1byte).unwrap();
    }
    unsafe { String::from_utf8_unchecked(hexdigest) }
}
