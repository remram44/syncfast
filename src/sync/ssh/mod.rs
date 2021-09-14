mod proto;

use futures::sink::Sink;
use futures::stream::{LocalBoxStream, StreamExt};
use std::collections::VecDeque;
use std::convert::{TryFrom, TryInto};
use std::future::Future;
use std::pin::Pin;
use std::process::Stdio;
use tokio::io::{AsyncWriteExt};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

use crate::Error;
use crate::streaming_iterator::StreamingIterator;
use crate::sync::{Destination, DestinationEvent, Source, SourceEvent};
use crate::sync::locations::SshLocation;
use crate::sync::ssh::proto::{OwnedMessage, Parser, write_message};

fn shell_escape(input: &str) -> String {
    let mut result = String::new();
    result.push('"');
    for c in input.chars() {
        if c == '\\' || c == '"' {
            result.push('\\');
        }
        result.push(c);
    }
    result.push('"');
    result
}

// First we define the SshStream and SshSink structs, which can read and write
// messages to/from a process.
// Then we implement SshSource and SshDestination, which run `remote-send` and
// `remote-recv` and use SshStream and SshSink to do all the messaging.

struct SshStream<'a> {
    stdout: &'a mut ChildStdout,
    parser: Parser,
    messages: VecDeque<OwnedMessage>,
}

impl<'a> SshStream<'a> {
    fn new(stdout: &'a mut ChildStdout) -> SshStream<'a> {
        SshStream {
            stdout,
            parser: Default::default(),
            messages: VecDeque::new(),
        }
    }

    fn project<'b>(self: &'b mut Pin<Box<SshStream<'a>>>) -> (&'b mut ChildStdout, &'b mut Parser, &'b mut VecDeque<OwnedMessage>) where 'a: 'b {
        unsafe {
            let s = self.as_mut().get_unchecked_mut();
            (s.stdout, &mut s.parser, &mut s.messages)
        }
    }

    fn stream<T: TryFrom<OwnedMessage, Error=()>>(mut arg: Pin<Box<SshStream<'a>>>) -> impl Future<Output=Option<(Result<T, Error>, Pin<Box<SshStream<'a>>>)>> {
        async move {
            let (stream, parser, messages) = arg.project();

            macro_rules! err {
                ($e:expr) => {
                    Some((Err($e.into()), arg))
                }
            }
            // FIXME: Replace by try_block when supported by Rust
            macro_rules! try_ {
                ($v:expr) => {
                    match $v {
                        Ok(r) => r,
                        Err(e) => return err!(e),
                    }
                }
            }

            if messages.is_empty() {
                // FIXME: Store the iterator instead of a vector of values,
                // however this makes it self-referential...
                let mut iterator = try_!(parser.read_async(stream).await);
                loop {
                    match iterator.next() {
                        Some(Ok(msg)) => messages.push_back(msg.into()),
                        Some(Err(e)) => return err!(std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
                        None => break,
                    }
                }
            }
            match messages.pop_front() {
                Some(msg) => {
                    let event = match msg.try_into() {
                        Ok(e) => e,
                        Err(()) => return err!(std::io::Error::new(std::io::ErrorKind::InvalidData, "Message is not valid for Source")),
                    };
                    Some((Ok(event), arg))
                }
                None => return None,
            }
        }
    }
}

struct SshSink<'a> {
    stdin: &'a mut ChildStdin,
    buffer: Vec<u8>,
}

impl<'a> SshSink<'a> {
    fn new(stdin: &'a mut ChildStdin) -> SshSink<'a> {
        SshSink {
            stdin,
            buffer: Vec::new(),
        }
    }

    fn project<'b>(self: &'b mut Pin<Box<SshSink<'a>>>) -> (&'b mut ChildStdin, &'b mut Vec<u8>) where 'a: 'b {
        unsafe {
            let s = self.as_mut().get_unchecked_mut();
            (s.stdin, &mut s.buffer)
        }
    }

    fn sink<T: Into<OwnedMessage>>(mut arg: Pin<Box<SshSink<'a>>>, event: T) -> impl Future<Output=Result<Pin<Box<SshSink<'a>>>, Error>> {
        async move {
            let (sink, mut buffer) = arg.project();

            write_message(&event.into(), &mut buffer)?;
            sink.write_all(buffer).await?;
            buffer.clear();
            Ok(arg)
        }
    }
}

pub struct SshSource {
    process: Child,
}

impl SshSource {
    pub fn new(loc: &SshLocation) -> Result<SshSource, Error> {
        let SshLocation { user, host, path } = loc;
        let connection_arg = match user {
            Some(user) => format!("{}@{}", user, host),
            None => host.to_owned(),
        };
        let process: Child = Command::new("ssh")
            .arg(connection_arg)
            .arg("syncfast")
            .arg("remote-send")
            .arg(shell_escape(path))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;
        Ok(SshSource { process })
    }
}

impl Source for SshSource {
    fn streams<'a>(&'a mut self) -> (LocalBoxStream<'a, Result<SourceEvent, Error>>, Pin<Box<dyn Sink<DestinationEvent, Error=Error> + 'a>>) {
        (
            futures::stream::unfold(
                Box::pin(SshStream::new(self.process.stdout.as_mut().unwrap())),
                SshStream::stream,
            ).boxed_local(),
            Box::pin(futures::sink::unfold(
                Box::pin(SshSink::new(self.process.stdin.as_mut().unwrap())),
                SshSink::sink,
            )),
        )
    }
}

pub struct SshDestination {
    process: Child,
}

impl SshDestination {
    pub fn new(loc: &SshLocation) -> Result<SshDestination, Error> {
        let SshLocation { user, host, path } = loc;
        let connection_arg = match user {
            Some(user) => format!("{}@{}", user, host),
            None => host.to_owned(),
        };
        let process: Child = Command::new("ssh")
            .arg(connection_arg)
            .arg("syncfast")
            .arg("remote-recv")
            .arg(shell_escape(path))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;
        Ok(SshDestination { process })
    }
}

impl Destination for SshDestination {
    fn streams<'a>(&'a mut self) -> (LocalBoxStream<'a, Result<DestinationEvent, Error>>, Pin<Box<dyn Sink<SourceEvent, Error=Error> + 'a>>) {
        (
            futures::stream::unfold(
                Box::pin(SshStream::new(self.process.stdout.as_mut().unwrap())),
                SshStream::stream,
            ).boxed_local(),
            Box::pin(futures::sink::unfold(
                Box::pin(SshSink::new(self.process.stdin.as_mut().unwrap())),
                SshSink::sink,
            )),
        )
    }
}
