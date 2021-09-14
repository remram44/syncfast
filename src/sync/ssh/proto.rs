use log::warn;
use std::convert::{TryFrom, TryInto};
use std::io::Write;
use std::ops::Deref;
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::HashDigest;
use crate::HASH_DIGEST_LEN;
use crate::streaming_iterator::StreamingIterator;
use crate::sync::{DestinationEvent, SourceEvent};

#[derive(Debug)]
pub struct Error(&'static str);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for Error {}

#[derive(Debug, PartialEq)]
pub enum Message<'a> {
    FileEntry(&'a [u8], usize, HashDigest),
    EndFiles,
    GetFile(&'a [u8]),
    FileStart(&'a [u8]),
    FileBlock(HashDigest, usize),
    FileEnd,
    GetBlock(HashDigest),
    BlockData(HashDigest, &'a [u8]),
    Complete,
}

#[derive(Debug, PartialEq)]
pub enum OwnedMessage {
    FileEntry(Vec<u8>, usize, HashDigest),
    EndFiles,
    GetFile(Vec<u8>),
    FileStart(Vec<u8>),
    FileBlock(HashDigest, usize),
    FileEnd,
    GetBlock(HashDigest),
    BlockData(HashDigest, Vec<u8>),
    Complete,
}

impl<'a> From<Message<'a>> for OwnedMessage {
    fn from(msg: Message<'a>) -> OwnedMessage {
        match msg {
            Message::FileEntry(name, size, digest) => OwnedMessage::FileEntry(name.to_owned(), size, digest),
            Message::EndFiles => OwnedMessage::EndFiles,
            Message::GetFile(name) => OwnedMessage::GetFile(name.to_owned()),
            Message::FileStart(name) => OwnedMessage::FileStart(name.to_owned()),
            Message::FileBlock(digest, size) => OwnedMessage::FileBlock(digest, size),
            Message::FileEnd => OwnedMessage::FileEnd,
            Message::GetBlock(digest) => OwnedMessage::GetBlock(digest),
            Message::BlockData(digest, data) => OwnedMessage::BlockData(digest, data.to_owned()),
            Message::Complete => OwnedMessage::Complete,
        }
    }
}

impl<'a> From<&'a OwnedMessage> for Message<'a> {
    fn from(msg: &'a OwnedMessage) -> Message<'a> {
        match msg {
            &OwnedMessage::FileEntry(ref name, size, ref digest) => Message::FileEntry(name, size, digest.clone()),
            &OwnedMessage::EndFiles => Message::EndFiles,
            &OwnedMessage::GetFile(ref name) => Message::GetFile(name),
            &OwnedMessage::FileStart(ref name) => Message::FileStart(name),
            &OwnedMessage::FileBlock(ref digest, size) => Message::FileBlock(digest.clone(), size),
            &OwnedMessage::FileEnd => Message::FileEnd,
            &OwnedMessage::GetBlock(ref digest) => Message::GetBlock(digest.clone()),
            &OwnedMessage::BlockData(ref digest, ref data) => Message::BlockData(digest.clone(), data),
            &OwnedMessage::Complete => Message::Complete,
        }
    }
}

impl From<SourceEvent> for OwnedMessage {
    fn from(event: SourceEvent) -> OwnedMessage {
        match event {
            SourceEvent::FileEntry(name, size, hash) => OwnedMessage::FileEntry(name, size, hash),
            SourceEvent::EndFiles => OwnedMessage::EndFiles,
            SourceEvent::FileStart(name) => OwnedMessage::FileStart(name),
            SourceEvent::FileBlock(hash, size) => OwnedMessage::FileBlock(hash, size),
            SourceEvent::FileEnd => OwnedMessage::FileEnd,
            SourceEvent::BlockData(hash, data) => OwnedMessage::BlockData(hash, data),
        }
    }
}

impl From<DestinationEvent> for OwnedMessage {
    fn from(event: DestinationEvent) -> OwnedMessage {
        match event {
            DestinationEvent::GetFile(name) => OwnedMessage::GetFile(name),
            DestinationEvent::GetBlock(digest) => OwnedMessage::GetBlock(digest),
            DestinationEvent::Complete => OwnedMessage::Complete,
        }
    }
}

impl TryFrom<OwnedMessage> for SourceEvent {
    type Error = ();

    fn try_from(message: OwnedMessage) -> Result<SourceEvent, ()> {
        Ok(match message {
            OwnedMessage::FileEntry(name, size, hash) => SourceEvent::FileEntry(name, size, hash),
            OwnedMessage::EndFiles => SourceEvent::EndFiles,
            OwnedMessage::FileStart(name) => SourceEvent::FileStart(name),
            OwnedMessage::FileBlock(hash, size) => SourceEvent::FileBlock(hash, size),
            OwnedMessage::FileEnd => SourceEvent::FileEnd,
            OwnedMessage::BlockData(hash, data) => SourceEvent::BlockData(hash, data),
            _ => return Err(()),
        })
    }
}

impl TryFrom<OwnedMessage> for DestinationEvent {
    type Error = ();

    fn try_from(message: OwnedMessage) -> Result<DestinationEvent, ()> {
        Ok(match message {
            OwnedMessage::GetFile(name) => DestinationEvent::GetFile(name),
            OwnedMessage::GetBlock(digest) => DestinationEvent::GetBlock(digest),
            OwnedMessage::Complete => DestinationEvent::Complete,
            _ => return Err(()),
        })
    }
}

pub fn write_message<'a, M: Into<Message<'a>>, W: Write>(message: M, mut writer: W) -> std::io::Result<()> {
    let message = message.into();
    match message {
        Message::FileEntry(name, size, digest) => {
            writer.write_all(b"FILE_ENTRY\n")?;
            writer.write_all(name)?;
            write!(writer, "\n{}\n", size)?;
            writer.write_all(&digest.0)?;
            writer.write_all(b"\n")?;
        }
        Message::EndFiles => {
            writer.write_all(b"END_FILES\n")?;
        }
        Message::GetFile(name) => {
            writer.write_all(b"GET_FILE\n")?;
            writer.write_all(name)?;
            writer.write_all(b"\n")?;
        }
        Message::FileStart(name) => {
            writer.write_all(b"FILE_START\n")?;
            writer.write_all(name)?;
            writer.write_all(b"\n")?;
        }
        Message::FileBlock(digest, size) => {
            writer.write_all(b"FILE_BLOCK\n")?;
            writer.write_all(&digest.0)?;
            write!(writer, "\n{}\n", size)?;
        }
        Message::FileEnd => {
            writer.write_all(b"FILE_END\n")?;
        }
        Message::GetBlock(digest) => {
            writer.write_all(b"GET_BLOCK\n")?;
            writer.write_all(&digest.0)?;
            writer.write_all(b"\n")?;
        }
        Message::BlockData(digest, data) => {
            writer.write_all(b"BLOCK_DATA")?;
            writer.write_all(&digest.0)?;
            write!(writer, "\n{}\n", data.len())?;
            writer.write_all(data)?;
            writer.write_all(b"\n")?;
        }
        Message::Complete => {
            writer.write_all(b"COMPLETE\n")?;
        }
    }
    Ok(())
}

#[derive(Default)]
pub struct Parser {
    buffer: Vec<u8>,
    pos: usize,
}

use std::future::Future;

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

    pub fn receive_async<
        'a, 'b,
        E,
        P: Future<Output=Result<(), E>>,
        F: 'b + FnOnce(&mut Vec<u8>) -> P,
    >(&'a mut self, func: F) -> impl Future<Output=Result<Messages<'a>, E>> + 'b
    where
        'a: 'b
    {
        async move {
            self.buffer.drain(..self.pos);
            self.pos = 0;

            func(&mut self.buffer).await?;
            Ok(Messages {
                buffer: &mut self.buffer,
                pos: &mut self.pos,
            })
        }
    }

    pub fn read_async<'a, R: AsyncRead + Unpin>(
        &'a mut self,
        mut reader: R,
    ) -> impl Future<Output=Result<Messages<'a>, std::io::Error>> {
        async move {
            self.buffer.drain(..self.pos);
            self.pos = 0;

            reader.read_buf(&mut self.buffer).await?;
            Ok(Messages {
                buffer: &mut self.buffer,
                pos: &mut self.pos,
            })
        }
    }

    pub fn parse<'a>(&'a mut self, input: &[u8]) -> Messages<'a> {
        self.buffer.drain(..self.pos);
        self.pos = 0;
        self.buffer.extend_from_slice(input);
        Messages {
            buffer: &mut self.buffer,
            pos: &mut self.pos,
        }
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

    fn read_exact<E>(
        &mut self,
        size: usize,
        error: E,
    ) -> Result<Option<&'a [u8]>, E> {
        if self.slice[self.pos..].len() >= size + 1 {
            if self.slice[self.pos + size] == b'\n' {
                let value = &self.slice[self.pos..self.pos + size];
                self.advance(size + 1);
                Ok(Some(value))
            } else {
                Err(error)
            }
        } else {
            Ok(None)
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
        let ret = if command == b"FILE_ENTRY" {
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
            let digest = match buffer.read_exact(HASH_DIGEST_LEN, Error("Unterminated digest")) {
                Err(e) => return Some(Err(e)),
                Ok(Some(s)) => s,
                Ok(None) => return None,
            };
            let digest = HashDigest(digest.try_into().unwrap());
            // Success
            Message::FileEntry(filename, size, digest)
        } else if command == b"END_FILES" {
            Message::EndFiles
        } else if command == b"GET_FILE" {
            // Read filename
            let filename = match buffer.read_line(FILENAME_MAX, Error("Unterminated filename")) {
                Err(e) => return Some(Err(e)),
                Ok(Some(s)) => s,
                Ok(None) => return None,
            };
            // Success
            Message::GetFile(filename)
        } else if command == b"FILE_START" {
            // Read filename
            let filename = match buffer.read_line(FILENAME_MAX, Error("Unterminated filename")) {
                Err(e) => return Some(Err(e)),
                Ok(Some(s)) => s,
                Ok(None) => return None,
            };
            // Success
            Message::FileStart(filename)
        } else if command == b"FILE_BLOCK" {
            // Read digest
            let digest = match buffer.read_exact(HASH_DIGEST_LEN, Error("Unterminated digest")) {
                Err(e) => return Some(Err(e)),
                Ok(Some(s)) => s,
                Ok(None) => return None,
            };
            let digest = HashDigest(digest.try_into().unwrap());
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
            Message::FileBlock(digest, size)
        } else if command == b"FILE_END" {
            Message::FileEnd
        } else if command == b"GET_BLOCK" {
            // Read digest
            let digest = match buffer.read_exact(HASH_DIGEST_LEN, Error("Unterminated digest")) {
                Err(e) => return Some(Err(e)),
                Ok(Some(s)) => s,
                Ok(None) => return None,
            };
            let digest = HashDigest(digest.try_into().unwrap());
            // Success
            Message::GetBlock(digest)
        } else if command == b"BLOCK_DATA" {
            // Read digest
            let digest = match buffer.read_exact(HASH_DIGEST_LEN, Error("Unterminated digest")) {
                Err(e) => return Some(Err(e)),
                Ok(Some(s)) => s,
                Ok(None) => return None,
            };
            let digest = HashDigest(digest.try_into().unwrap());
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
            let data = match buffer.read_exact(size, Error("Invalid data end byte")) {
                Err(e) => return Some(Err(e)),
                Ok(Some(s)) => s,
                Ok(None) => return None,
            };
            // Success
            Message::BlockData(digest, data)
        } else if command == b"COMPLETE" {
            Message::Complete
        } else {
            warn!("Unknown command: {:?}", command);
            return Some(Err(Error("Unknown command")));
        };

        *self.pos += buffer.pos;
        Some(Ok(ret))
    }
}

#[cfg(test)]
mod tests {
    use super::{OwnedMessage, Parser, Message, Messages, write_message};
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

    #[test]
    fn test_write() {
        let mut output = Vec::new();
        write_message(
            Message::FileEntry(b"filename", 12, HashDigest(*b"12345678901234567890")),
            &mut output,
        ).unwrap();
        write_message(
            &OwnedMessage::EndFiles,
            &mut output,
        ).unwrap();
        assert_eq!(
            &output,
            b"FILE_ENTRY\nfilename\n12\n12345678901234567890\nEND_FILES\n",
        );
    }
}
