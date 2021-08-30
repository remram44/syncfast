use std::convert::TryInto;
use std::ops::Deref;

use crate::HashDigest;
use crate::HASH_DIGEST_LEN;
use crate::streaming_iterator::StreamingIterator;
use crate::sync::Message;

#[derive(Debug)]
pub struct Error(&'static str);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for Error {}

#[derive(Default)]
pub struct Parser {
    buffer: Vec<u8>,
    pos: usize,
}

impl Parser {
    pub fn receive<'a, E, F>(&'a mut self, func: F) -> Result<Messages<'a>, E>
    where
        F: FnOnce(&mut Vec<u8>) -> Result<(), E>
    {
        self.buffer.drain(..self.pos);
        self.pos = 0;

        func(&mut self.buffer)?;
        Ok(Messages {
            buffer: &mut self.buffer,
            pos: &mut self.pos,
        })
    }
}

pub struct Messages<'a> {
    buffer: &'a mut Vec<u8>,
    pos: &'a mut usize,
}

const COMMAND_MAX: usize = 20;
const FILENAME_MAX: usize = 100;
const SIZE_MAX: usize = 15;

struct View<'a, T> {
    slice: &'a [T],
    pos: usize,
}

impl<'a, T> View<'a, T> {
    fn new(slice: &'a [T]) -> View<'a, T> {
        View {
            slice,
            pos: 0,
        }
    }

    fn advance(&mut self, offset: usize) -> &'a [T] {
        assert!(self.pos + offset <= self.slice.len());
        let ret = &self.slice[self.pos..self.pos + offset];
        self.pos += offset;
        ret
    }
}

impl<'a> View<'a, u8> {
    fn read_line<E>(
        &mut self,
        max_size: usize,
        error: E,
    ) -> Result<Option<&'a [u8]>, E> {
        match self.slice[self.pos..self.slice.len().min(self.pos + max_size + 1)].iter().position(|&b| b == b'\n') {
            Some(s) => {
                let line = &self.slice[self.pos..self.pos + s];
                self.advance(s + 1);
                Ok(Some(line))
            }
            None => {
                if self.len() >= max_size {
                    Err(error)
                } else {
                    Ok(None)
                }
            }
        }
    }
}

impl<'a, T> Deref for View<'a, T> {
    type Target = [T];

    fn deref(&self) -> &[T] {
        &self.slice[self.pos..]
    }
}

impl<'a, 'b: 'a> StreamingIterator<'a> for Messages<'b> {
    type Item = Result<Message<'a>, Error>;

    fn next(&'a mut self) -> Option<Result<Message<'a>, Error>> {
        let mut buffer = View::new(&self.buffer[*self.pos..]);
        if buffer.len() == 0 {
            return None;
        }
        // Read command
        let command = match buffer.read_line(COMMAND_MAX, Error("Unterminated command")) {
            Err(e) => return Some(Err(e)),
            Ok(Some(s)) => s,
            Ok(None) => return None,
        };
        if command == b"FILE_ENTRY" {
            // Read filename
            let filename = match buffer.read_line(FILENAME_MAX, Error("Unterminated filename")) {
                Err(e) => return Some(Err(e)),
                Ok(Some(s)) => s,
                Ok(None) => return None,
            };
            // Read size
            let size = match buffer.read_line(SIZE_MAX, Error("Unterminated size")) {
                Err(e) => return Some(Err(e)),
                Ok(Some(s)) => s,
                Ok(None) => return None,
            };
            let size: Option<&str> = std::str::from_utf8(size).ok();
            let size: Option<usize> = size.and_then(|s| s.parse().ok());
            let size = match size {
                Some(s) => s,
                None => return Some(Err(Error("Invalid file size"))),
            };
            // Read digest
            let digest = match buffer.read_line(HASH_DIGEST_LEN, Error("Unterminated digest")) {
                Err(e) => return Some(Err(e)),
                Ok(Some(s)) => s,
                Ok(None) => return None,
            };
            let digest = if digest.len() == HASH_DIGEST_LEN {
                HashDigest(digest.try_into().unwrap())
            } else {
                return None;
            };
            // Success
            *self.pos += buffer.pos;
            Some(Ok(Message::FileEntry(filename, size, digest)))
        } else if command == b"END_FILES" {
            *self.pos += buffer.pos;
            Some(Ok(Message::EndFiles))
        } else if command == b"GET_FILE" {
            // Read filename
            let filename = match buffer.read_line(FILENAME_MAX, Error("Unterminated filename")) {
                Err(e) => return Some(Err(e)),
                Ok(Some(s)) => s,
                Ok(None) => return None,
            };
            // Success
            *self.pos += buffer.pos;
            Some(Ok(Message::GetFile(filename)))
        } else if command == b"FILE_START" {
            // Read filename
            let filename = match buffer.read_line(FILENAME_MAX, Error("Unterminated filename")) {
                Err(e) => return Some(Err(e)),
                Ok(Some(s)) => s,
                Ok(None) => return None,
            };
            // Success
            *self.pos += buffer.pos;
            Some(Ok(Message::FileStart(filename)))
        } else if command == b"FILE_BLOCK" {
            // Read digest
            let digest = match buffer.read_line(HASH_DIGEST_LEN, Error("Unterminated digest")) {
                Err(e) => return Some(Err(e)),
                Ok(Some(s)) => s,
                Ok(None) => return None,
            };
            let digest = if digest.len() == HASH_DIGEST_LEN {
                HashDigest(digest.try_into().unwrap())
            } else {
                return None;
            };
            // Read size
            let size = match buffer.read_line(SIZE_MAX, Error("Unterminated size")) {
                Err(e) => return Some(Err(e)),
                Ok(Some(s)) => s,
                Ok(None) => return None,
            };
            let size: Option<&str> = std::str::from_utf8(size).ok();
            let size: Option<usize> = size.and_then(|s| s.parse().ok());
            let size = match size {
                Some(s) => s,
                None => return Some(Err(Error("Invalid block size"))),
            };
            // Success
            *self.pos += buffer.pos;
            Some(Ok(Message::FileBlock(digest, size)))
        } else if command == b"FILE_END" {
            *self.pos += buffer.pos;
            Some(Ok(Message::FileEnd))
        } else if command == b"GET_BLOCK" {
            // Read digest
            let digest = match buffer.read_line(HASH_DIGEST_LEN, Error("Unterminated digest")) {
                Err(e) => return Some(Err(e)),
                Ok(Some(s)) => s,
                Ok(None) => return None,
            };
            let digest = if digest.len() == HASH_DIGEST_LEN {
                HashDigest(digest.try_into().unwrap())
            } else {
                return None;
            };
            // Success
            *self.pos += buffer.pos;
            Some(Ok(Message::GetBlock(digest)))
        } else if command == b"BLOCK_DATA" {
            // Read data length
            let size = match buffer.read_line(SIZE_MAX, Error("Unterminated length")) {
                Err(e) => return Some(Err(e)),
                Ok(Some(s)) => s,
                Ok(None) => return None,
            };
            let size: Option<&str> = std::str::from_utf8(size).ok();
            let size: Option<usize> = size.and_then(|s| s.parse().ok());
            let size = match size {
                Some(s) => s,
                None => return Some(Err(Error("Invalid block length"))),
            };
            // Read data
            let data = if buffer.len() >= size + 1 {
                let data = buffer.advance(size);
                if buffer.advance(1) != b"\n" {
                    return Some(Err(Error("Invalid data end byte")));
                }
                data
            } else {
                return None;
            };
            // Success
            *self.pos += buffer.pos;
            Some(Ok(Message::BlockData(data)))
        } else if command == b"COMPLETE" {
            *self.pos += buffer.pos;
            Some(Ok(Message::Complete))
        } else {
            Some(Err(Error("Unknown command: {:?}")))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Parser, Message, Messages};
    use crate::HashDigest;
    use crate::streaming_iterator::StreamingIterator;

    fn compare<'a>(mut iterator: Messages<'a>, expected: &[Message<'static>]) {
        let mut expected = expected.iter();
        loop {
            match (iterator.next(), expected.next()) {
                (None, None) => break,
                (Some(msg), Some(e)) => {
                    let msg = msg.unwrap();
                    assert_eq!(&msg, e);
                }
                (Some(msg), None) => {
                    let msg = msg.unwrap();
                    panic!("More messages than expected: {:?}", msg);
                }
                (None, Some(e)) => panic!("Fewer messages than expected: {:?}", e),
            }
        }
    }

    #[test]
    fn test_parse() {
        let inputs: &[&[u8]] = &[
            b"FILE_ENTR",
            b"Y",
            b"\n",
            b"filename\n12",
            b"\n12345678901234567890\nCOMPLETE",
            b"\n",
        ];
        let expected: &[&[Message<'static>]] = &[
            &[],
            &[],
            &[],
            &[],
            &[Message::FileEntry(
                b"filename", 12, HashDigest(*b"12345678901234567890"),
            )],
            &[Message::Complete],
        ];
        let mut parser: Parser = Default::default();
        for (bytes, expected_messages) in inputs.iter().zip(expected) {
            compare(
                parser.receive::<(), _>(|buf| { buf.extend_from_slice(bytes); Ok(()) }).unwrap(),
                expected_messages,
            );
        }
    }
}
