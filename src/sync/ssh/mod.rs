mod proto;

use futures::sink::Sink;
use futures::stream::{LocalBoxStream, StreamExt};
use std::future::Future;
use std::pin::Pin;
use std::process::Stdio;
use tokio::io::{AsyncWriteExt};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

use crate::Error;
use crate::sync::{Destination, DestinationEvent, Source, SourceEvent};
use crate::sync::locations::SshLocation;
use crate::sync::ssh::proto::{Messages, Parser, write_message};

struct SshStream<'a> {
    stdout: &'a mut ChildStdout,
    parser: Parser,
}

impl<'a> SshStream<'a> {
    fn new(stdout: &'a mut ChildStdout) -> SshStream<'a> {
        SshStream {
            stdout,
            parser: Default::default(),
        }
    }

    fn project<'b>(self: &'b mut Pin<Box<SshStream<'a>>>) -> (&'b mut ChildStdout, &'b mut Parser) where 'a: 'b {
        unsafe {
            let s = self.as_mut().get_unchecked_mut();
            (s.stdout, &mut s.parser)
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
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;
        Ok(SshSource { process })
    }

    fn stream<'a>(mut arg: Pin<Box<SshStream<'a>>>) -> impl Future<Output=Option<(Result<SourceEvent, Error>, Pin<Box<SshStream<'a>>>)>> {
        async move {
            let (stream, parser) = arg.project();

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

            let messages: Result<Messages, std::io::Error> = parser.read_async(stream).await;
            let messages: Messages = try_!(messages);
            let event = todo!();
            Some((Ok(event), arg))
        }
    }

    fn sink<'a>(mut arg: Pin<Box<SshSink<'a>>>, event: DestinationEvent) -> impl Future<Output=Result<Pin<Box<SshSink<'a>>>, Error>> {
        async move {
            let (sink, mut buffer) = arg.project();

            write_message(&event.into(), &mut buffer)?;
            sink.write_all(buffer).await?;
            buffer.clear();
            Ok(arg)
        }
    }
}

impl Source for SshSource {
    fn streams<'a>(&'a mut self) -> (LocalBoxStream<'a, Result<SourceEvent, Error>>, Pin<Box<dyn Sink<DestinationEvent, Error=Error> + 'a>>) {
        (
            futures::stream::unfold(
                Box::pin(SshStream::new(self.process.stdout.as_mut().unwrap())),
                SshSource::stream,
            ).boxed_local(),
            Box::pin(futures::sink::unfold(
                Box::pin(SshSink::new(self.process.stdin.as_mut().unwrap())),
                SshSource::sink,
            )),
        )
    }
}

pub struct SshDestination;

impl SshDestination {
    pub fn new(_loc: &SshLocation) -> Result<SshDestination, Error> {
        Ok(SshDestination)
    }
}

impl Destination for SshDestination {
    fn streams<'a>(&'a mut self) -> (LocalBoxStream<'a, Result<DestinationEvent, Error>>, Pin<Box<dyn Sink<SourceEvent, Error=Error>+ 'a>>) {
        todo!() // SshDestination
    }
}
