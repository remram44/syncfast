use std::cmp::min;
use std::io::{self, Read, Write};

pub trait ReadRetry: Read {
    /// Wrapper for `Read::read()` that retries when interrupted
    ///
    /// This will never fail with `ErrorKind::Interrupted`, and will only
    /// return less that the requested size if the end-of-file has been
    /// reached.
    /// The downside is that if an error is returned, an unknown number of
    /// bytes might have been read.
    fn read_retry(&mut self, buffer: &mut [u8]) -> io::Result<usize>;
}

impl<R: Read> ReadRetry for R {
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
}

#[derive(Clone, Copy)]
pub enum CopyMode {
    Exact(usize),
    Maximum(usize),
    All,
}

pub fn copy<R: Read, W: Write>(reader: &mut R, writer: &mut W,
                               mut what: CopyMode)
    -> io::Result<()>
{
    let mut buffer: [u8; 4096] = unsafe { ::std::mem::uninitialized() };
    while match what {
        CopyMode::All => true,
        CopyMode::Exact(l) | CopyMode::Maximum(l) => l > 0,
    } {
        let len = match what {
            CopyMode::Exact(len) | CopyMode::Maximum(len) => min(4096, len),
            CopyMode::All => 4096,
        };
        if try!(reader.read(&mut buffer[..len])) != len {
            if let CopyMode::Exact(_) = what {
                return Err(io::Error::new(io::ErrorKind::Other,
                                          "Unexpected end of file"));
            } else {
                return Ok(());
            }
        }
        try!(writer.write_all(&buffer[..len]));
        match what {
            CopyMode::All => {},
            CopyMode::Exact(ref mut l) | CopyMode::Maximum(ref mut l) => {
                *l -= len;
            }
        }
    }
    Ok(())
}

pub fn to_hex(sha1: &[u8]) -> String {
    let mut hexdigest = Vec::with_capacity(40);
    for sha1byte in sha1.iter() {
        write!(hexdigest, "{:02X}", sha1byte).unwrap();
    }
    info!("Read SHA-1: {}",
          unsafe { ::std::str::from_utf8_unchecked(&hexdigest) });
    unsafe { String::from_utf8_unchecked(hexdigest) }
}
