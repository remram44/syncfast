mod proto;

use futures::sink::Sink;
use futures::stream::{LocalBoxStream, StreamExt};
use log::{debug, info};
use std::collections::VecDeque;
use std::convert::{TryFrom, TryInto};
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::process::Stdio;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, Stdin, stdin, stdout};
use tokio::process::{Child, Command};

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

struct SshStream<'a, R: AsyncRead + Unpin> {
    stdout: &'a mut R,
    parser: Parser,
    messages: VecDeque<OwnedMessage>,
}

impl<'a, R: AsyncRead + Unpin> SshStream<'a, R> {
    fn new(stdout: &'a mut R) -> SshStream<'a, R> {
        SshStream {
            stdout,
            parser: Default::default(),
            messages: VecDeque::new(),
        }
    }

    fn project<'b>(self: &'b mut Pin<Box<SshStream<'a, R>>>) -> (&'b mut R, &'b mut Parser, &'b mut VecDeque<OwnedMessage>) where 'a: 'b {
        unsafe {
            let s = self.as_mut().get_unchecked_mut();
            (s.stdout, &mut s.parser, &mut s.messages)
        }
    }

    fn stream<T: TryFrom<OwnedMessage, Error=()> + Debug>(mut arg: Pin<Box<SshStream<'a, R>>>) -> impl Future<Output=Option<(Result<T, Error>, Pin<Box<SshStream<'a, R>>>)>> {
        async move {
            let (mut stream, parser, messages) = arg.project();

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

            let mut end = false;
            while messages.is_empty() && !end {
                // FIXME: Store the iterator instead of a vector of values,
                // however this makes it self-referential...
                let (mut iterator, end_) = try_!(parser.read_async(&mut stream).await);
                end = end_;
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
                        Err(()) => return err!(std::io::Error::new(std::io::ErrorKind::InvalidData, "Message is not valid for this mode")),
                    };
                    debug!("ssh: recv {:?}", event);
                    Some((Ok(event), arg))
                }
                None => return None,
            }
        }
    }
}

struct SshSink<W: AsyncWrite + Unpin> {
    stdin: W,
    buffer: Vec<u8>,
}

impl<W: AsyncWrite + Unpin> SshSink<W> {
    fn new(stdin: W) -> SshSink< W> {
        SshSink {
            stdin,
            buffer: Vec::new(),
        }
    }

    fn project<'a>(self: &'a mut Pin<Box<SshSink< W>>>) -> (&'a mut W, &'a mut Vec<u8>) {
        unsafe {
            let s = self.as_mut().get_unchecked_mut();
            (&mut s.stdin, &mut s.buffer)
        }
    }

    fn sink<T: Into<OwnedMessage> + Debug>(mut arg: Pin<Box<SshSink<W>>>, event: T) -> impl Future<Output=Result<Pin<Box<SshSink<W>>>, Error>> {
        async move {
            let (sink, mut buffer) = arg.project();

            debug!("ssh: send {:?}", event);
            write_message(&event.into(), &mut buffer)?;
            //eprintln!("send \"{}\"", String::from_utf8_lossy(&buffer));
            sink.write_all(buffer).await?;
            sink.flush().await?;
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
            Some(user) => {
                info!("Setting up source {}@{}:{}", user, host, path);
                format!("{}@{}", user, host)
            }
            None => {
                info!("Setting up source {}:{}", host, path);
                host.to_owned()
            }
        };
        let escaped_path = shell_escape(path);
        debug!(
            "Running command: ssh {} syncfast remote-send {}",
            connection_arg, escaped_path,
        );
        let process: Child = Command::new("ssh")
            .arg(connection_arg)
            .arg("tee /tmp/input.bin | ")
            .arg("syncfast")
            .arg("-v -v")
            .arg("remote-send")
            .arg(escaped_path)
            .arg("2>/tmp/remote.log")
            .arg(" | tee /tmp/output.bin")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;
        Ok(SshSource { process })
    }
}

impl Source for SshSource {
    fn streams<'a>(&'a mut self) -> (LocalBoxStream<'a, Result<SourceEvent, Error>>, Pin<Box<dyn Sink<DestinationEvent, Error=Error> + 'a>>) {
        let stdin = match self.process.stdin.take() {
            Some(s) => s,
            None => panic!("Called streams() twice"),
        };
        (
            futures::stream::unfold(
                Box::pin(SshStream::new(self.process.stdout.as_mut().unwrap())),
                SshStream::stream,
            ).boxed_local(),
            Box::pin(futures::sink::unfold(
                Box::pin(SshSink::new(stdin)),
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
            Some(user) => {
                info!("Setting up destination {}@{}:{}", user, host, path);
                format!("{}@{}", user, host)
            }
            None => {
                info!("Setting up destination {}:{}", host, path);
                host.to_owned()
            }
        };
        let escaped_path = shell_escape(path);
        debug!(
            "Running command: ssh {} syncfast remote-recv {}",
            connection_arg, escaped_path,
        );
        let process: Child = Command::new("ssh")
            .arg(connection_arg)
            .arg("tee /tmp/input.bin | ")
            .arg("syncfast")
            .arg("-v -v")
            .arg("remote-recv")
            .arg(escaped_path)
            .arg("2>/tmp/remote.log")
            .arg(" | tee /tmp/output.bin")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;
        Ok(SshDestination { process })
    }
}

impl Destination for SshDestination {
    fn streams<'a>(&'a mut self) -> (LocalBoxStream<'a, Result<DestinationEvent, Error>>, Pin<Box<dyn Sink<SourceEvent, Error=Error> + 'a>>) {
        let stdin = match self.process.stdin.take() {
            Some(s) => s,
            None => panic!("Called streams() twice"),
        };
        (
            futures::stream::unfold(
                Box::pin(SshStream::new(self.process.stdout.as_mut().unwrap())),
                SshStream::stream,
            ).boxed_local(),
            Box::pin(futures::sink::unfold(
                Box::pin(SshSink::new(stdin)),
                SshSink::sink,
            )),
        )
    }
}

pub struct StdioEndpoint {
    stdin: Stdin,
}

impl StdioEndpoint {
    pub fn new() -> StdioEndpoint {
        StdioEndpoint {
            stdin: stdin(),
        }
    }
}

impl Source for StdioEndpoint {
    fn streams<'a>(&'a mut self) -> (LocalBoxStream<'a, Result<SourceEvent, Error>>, Pin<Box<dyn Sink<DestinationEvent, Error=Error> + 'a>>) {
        (
            futures::stream::unfold(
                Box::pin(SshStream::new(&mut self.stdin)),
                SshStream::stream,
            ).boxed_local(),
            Box::pin(futures::sink::unfold(
                Box::pin(SshSink::new(stdout())),
                SshSink::sink,
            )),
        )
    }
}

impl Destination for StdioEndpoint {
    fn streams<'a>(&'a mut self) -> (LocalBoxStream<'a, Result<DestinationEvent, Error>>, Pin<Box<dyn Sink<SourceEvent, Error=Error> + 'a>>) {
        (
            futures::stream::unfold(
                Box::pin(SshStream::new(&mut self.stdin)),
                SshStream::stream,
            ).boxed_local(),
            Box::pin(futures::sink::unfold(
                Box::pin(SshSink::new(stdout())),
                SshSink::sink,
            )),
        )
    }
}
