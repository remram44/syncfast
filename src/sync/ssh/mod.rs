mod proto;

use futures::io::{AsyncWrite, AsyncWriteExt};
use futures::sink::Sink;
use futures::stream::{LocalBoxStream, StreamExt};
use std::future::Future;
use std::pin::Pin;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use crate::Error;
use crate::sync::{Destination, DestinationEvent, Source, SourceEvent};
use crate::sync::locations::SshLocation;
use crate::sync::ssh::proto::{Parser, write_message};

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

    fn project_sink<'a, 'b>(sink: &'b mut Pin<Box<(&'a mut ChildStdin, Vec<u8>)>>) -> (&'b mut ChildStdin, &'b mut Vec<u8>) where 'a: 'b {
        unsafe {
            let s = sink.as_mut().get_unchecked_mut();
            (s.0, &mut s.1)
        }
    }

    fn stream<'a>(mut stream: Pin<Box<(&'a mut ChildStdout, Parser)>>) -> impl Future<Output=Option<(Result<SourceEvent, Error>, Pin<Box<(&'a mut ChildStdout, Parser)>>)>> {
        async move {
            let parser = &mut stream.1;
            let stream = &mut stream.0;
            todo!()
        }
    }

    fn sink<'a>(mut sink: Pin<Box<(&'a mut ChildStdin, Vec<u8>)>>, event: DestinationEvent) -> impl Future<Output=Result<Pin<Box<(&'a mut ChildStdin, Vec<u8>)>>, Error>> {
        async move {
            let (sink, mut buffer) = Self::project_sink(&mut sink);
            write_message(&event.into(), &mut buffer)?;
            AsyncWriteExt::write_all(sink, buffer).await?;
            buffer.clear();
            todo!()
        }
    }
}

impl Source for SshSource {
    fn streams<'a>(&'a mut self) -> (LocalBoxStream<'a, Result<SourceEvent, Error>>, Pin<Box<dyn Sink<DestinationEvent, Error=Error> + 'a>>) {
        let parser: Parser = Default::default();
        (
            futures::stream::unfold(
                Box::pin((self.process.stdout.as_mut().unwrap(), parser)),
                SshSource::stream,
            ).boxed_local(),
            Box::pin(futures::sink::unfold(
                Box::pin((self.process.stdin.as_mut().unwrap(), Vec::new())),
                SshSource::sink,
            )),
        )
    }
}

struct SshSourceFrom {
    stdout: ChildStdout,
}

pub struct SshDestination;

impl SshDestination {
    pub fn new(loc: &SshLocation) -> Result<SshDestination, Error> {
        Ok(SshDestination)
    }
}

impl Destination for SshDestination {
    fn streams<'a>(&'a mut self) -> (LocalBoxStream<'a, Result<DestinationEvent, Error>>, Pin<Box<dyn Sink<SourceEvent, Error=Error>+ 'a>>) {
        todo!() // SshDestination
    }
}
