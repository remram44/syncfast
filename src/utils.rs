use std::io::{self, Read};

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
