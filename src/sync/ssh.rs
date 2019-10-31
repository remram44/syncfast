use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::thread;

use crate::{Error, HashDigest};
use crate::locations::SshLocation;
use crate::sync::{IndexEvent, Sink, SinkWrapper, Source, SourceWrapper};

fn run_ssh(ssh: &SshLocation, args: &[&str]) -> std::io::Result<Child> {
    let mut cmd = Command::new("ssh");
    match &ssh.user {
        Some(user) => cmd.arg(format!("{}@{}", user, ssh.host)),
        None => cmd.arg(&ssh.host),
    };
    let child = cmd
        .arg("rrsync")
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    Ok(child)
}

pub struct SshSink<'a> {
    location: &'a SshLocation,
    child: Child,
}

impl<'a> Sink for SshSink<'a> {
    // TODO: Implement SshSink
    fn new_file(
        &mut self,
        name: &Path,
        modified: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), Error> {
        unimplemented!()
    }

    fn new_block(
        &mut self,
        hash: &HashDigest,
        size: usize,
    ) -> Result<(), Error> {
        unimplemented!()
    }

    fn end_files(&mut self) -> Result<(), Error> {
        unimplemented!()
    }

    fn feed_block(
        &mut self,
        hash: &HashDigest,
        block: &[u8],
    ) -> Result<(), Error> {
        unimplemented!()
    }

    fn next_requested_block(&mut self) -> Result<Option<HashDigest>, Error> {
        unimplemented!()
    }

    fn is_missing_blocks(&self) -> Result<bool, Error> {
        unimplemented!()
    }
}

pub struct SshSource<'a> {
    location: &'a SshLocation,
    child: Child,
    read_thread: thread::JoinHandle<()>,
    read_channel: mpsc::Receiver<i32>,
}

impl<'a> Source for SshSource<'a> {
    // TODO: Implement SshSource
    fn next_from_index(&mut self) -> Result<Option<IndexEvent>, Error> {
        unimplemented!()
    }

    fn request_block(&mut self, hash: &HashDigest) -> Result<(), Error> {
        unimplemented!()
    }

    fn get_next_block(
        &mut self,
    ) -> Result<Option<(HashDigest, Vec<u8>)>, Error> {
        unimplemented!()
    }
}

pub struct SshWrapper(pub SshLocation);

impl SinkWrapper for SshWrapper {
    fn open<'a>(&'a mut self) -> Result<Box<dyn Sink + 'a>, Error> {
        let child = run_ssh(&self.0, &["piped-sink"])?;
        Ok(Box::new(SshSink {
            location: &self.0,
            child,
        }))
    }
}

impl SourceWrapper for SshWrapper {
    fn open<'a>(&'a mut self) -> Result<Box<dyn Source + 'a>, Error> {
        let mut child = run_ssh(&self.0, &["piped-source"])?;
        let stdout = child.stdout.take().unwrap();
        let stderr = child.stdout.take().unwrap();
        let (tx, rx) = mpsc::sync_channel(1);
        let read_thread = thread::spawn(move || {
            drop(stdout);
            drop(stderr);
            tx.send(1).unwrap();
        });
        Ok(Box::new(SshSource {
            location: &self.0,
            child,
            read_thread,
            read_channel: rx,
        }))
    }
}
