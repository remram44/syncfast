mod proto;

use futures::stream::StreamExt;
use log::{debug, info};
use std::collections::VecDeque;
use std::convert::{TryFrom, TryInto};
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::process::Stdio;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, stdin, stdout};
use tokio::process::{Child, Command};

use crate::Error;
use crate::streaming_iterator::StreamingIterator;
use crate::sync::{Destination, Source};
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

struct SshStream<R: AsyncRead + Unpin> {
    stdout: R,
    parser: Parser,
    messages: VecDeque<OwnedMessage>,
}

impl<R: AsyncRead + Unpin> SshStream<R> {
    fn new(stdout: R) -> SshStream<R> {
        SshStream {
            stdout,
            parser: Default::default(),
            messages: VecDeque::new(),
        }
    }

    fn project<'b>(self: &'b mut Pin<Box<SshStream<R>>>) -> (&'b mut R, &'b mut Parser, &'b mut VecDeque<OwnedMessage>) where R: 'b {
        unsafe {
            let s = self.as_mut().get_unchecked_mut();
            (&mut s.stdout, &mut s.parser, &mut s.messages)
        }
    }

    fn stream<T: TryFrom<OwnedMessage, Error=()> + Debug>(mut arg: Pin<Box<SshStream<R>>>) -> impl Future<Output=Option<(Result<T, Error>, Pin<Box<SshStream< R>>>)>> {
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
                        Some(Err(e)) => return err!(e),
                        None => break,
                    }
                }
            }
            match messages.pop_front() {
                Some(msg) => {
                    let event = match msg.try_into() {
                        Ok(e) => e,
                        Err(()) => return err!(Error::Protocol(Box::new(
                            proto::Error("Message is not valid for this mode"),
                        ))),
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

pub fn ssh_source(loc: &SshLocation) -> Result<Source, Error> {
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
        .arg("syncfast")
        .arg("remote-send")
        .arg(escaped_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()?;

    Ok(Source {
        stream: futures::stream::unfold(
            Box::pin(SshStream::new(process.stdout.unwrap())),
            SshStream::stream,
        ).boxed_local(),
        sink: Box::pin(futures::sink::unfold(
            Box::pin(SshSink::new(process.stdin.unwrap())),
            SshSink::sink,
        )),
    })
}

pub fn ssh_destination(loc: &SshLocation) -> Result<Destination, Error> {
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
        .arg("syncfast")
        .arg("remote-recv")
        .arg(escaped_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()?;

    Ok(Destination {
        stream: futures::stream::unfold(
            Box::pin(SshStream::new(process.stdout.unwrap())),
            SshStream::stream,
        ).boxed_local(),
        sink: Box::pin(futures::sink::unfold(
            Box::pin(SshSink::new(process.stdin.unwrap())),
            SshSink::sink,
        )),
    })
}

pub fn stdio_source() -> Source {
    Source {
        stream: futures::stream::unfold(
            Box::pin(SshStream::new(stdin())),
            SshStream::stream,
        ).boxed_local(),
        sink: Box::pin(futures::sink::unfold(
            Box::pin(SshSink::new(stdout())),
            SshSink::sink,
        )),
    }
}

pub fn stdio_destination() -> Destination {
    Destination {
        stream: futures::stream::unfold(
            Box::pin(SshStream::new(stdin())),
            SshStream::stream,
        ).boxed_local(),
        sink: Box::pin(futures::sink::unfold(
            Box::pin(SshSink::new(stdout())),
            SshSink::sink,
        )),
    }
}
