mod proto;

use std::io::{BufRead, BufReader, Read, Write};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::thread;

use crate::{Error, HashDigest};
use crate::locations::SshLocation;
use crate::sync::{Sink, SinkEvent, SinkWrapper,
                  Source, SourceEvent, SourceWrapper};
use self::proto::{CommunicationError, SyncReader, path_from_u8, path_to_u8};

/// The wrapper for SSH endpoints
pub struct SshWrapper(pub SshLocation);

/// Run an SSH command with stdio piped and the given destination and args
fn run_ssh(ssh: &SshLocation, args: &[&str]) -> std::io::Result<Child> {
    let mut cmd = Command::new("ssh");
    match &ssh.user {
        Some(user) => cmd.arg(format!("{}@{}", user, ssh.host)),
        None => cmd.arg(&ssh.host),
    };
    cmd
        .arg("/rrsync/debug/rrsync")
        .arg("-v")
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    info!("{:?}", cmd);
    cmd.spawn()
}

/// Read from stderr, print it here with a prefix
fn recv_errors<R: Read>(stderr: R, prefix: &'static str) {
    let mut stderr = BufReader::new(stderr);
    let mut buffer = String::new();
    let r: std::io::Result<()> = (|| {
        while stderr.read_line(&mut buffer)? > 0 {
            eprint!("remote {}: {}", prefix, buffer);
            buffer.clear();
        }
        Ok(())
    })();
    if let Err(e) = r {
        error!("{},  error reading stderr: {}", prefix, e);
    }
}

/// Sink writing to a remote machine via SSH
pub struct SshSink<W: Write> {
    child: Option<Child>,
    writer: W,
    rx: mpsc::Receiver<SinkEvent>,
    done: bool,
}

impl<W: Write> Drop for SshSink<W> {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            // Join SSH process
            match child.wait() {
                Ok(s) => {
                    if !s.success() {
                        error!("SSH to destination exited with {:?}", s);
                    }
                }
                Err(e) => {
                    error!(
                        "Error waiting on SSH process to destination: {}",
                        e,
                    );
                }
            }
        }
    }
}

impl<W: Write> SshSink<W> {
    pub fn piped<R>(stdin: W, stdout: R) -> SshSink<W>
        where R: Read + Send + 'static
    {
        let (tx, rx) = mpsc::sync_channel(1);
        thread::spawn(move || recv_from_sink(stdout, tx));
        SshSink {
            child: None,
            writer: stdin,
            rx,
            done: false,
        }
    }

    fn new_file(
        &mut self,
        name: &Path,
        modified: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), Error> {
        let path = path_to_u8(name);
        write!(self.writer, "FILE {}:", path.len())?;
        self.writer.write_all(&path_to_u8(name))?;
        writeln!(self.writer, " {}", modified.timestamp())?;
        Ok(())
    }

    fn new_block(
        &mut self,
        hash: &HashDigest,
        size: usize,
    ) -> Result<(), Error> {
        writeln!(self.writer, "BLOCK 40:{} {}", hash, size)?;
        Ok(())
    }

    fn feed_block(
        &mut self,
        hash: &HashDigest,
        block: &[u8],
    ) -> Result<(), Error> {
        write!(self.writer, "DATA 40:{} {}:", hash, block.len())?;
        self.writer.write_all(block)?;
        self.writer.write_all(b"\n")?;
        Ok(())
    }
}

impl<W: Write> Sink for SshSink<W> {
    fn next_event(&mut self) -> Result<Option<SinkEvent>, Error> {
        if self.done {
            return Ok(None)
        }

        Ok(match self.rx.recv() {
            Ok(SinkEvent::End) => {
                self.done = true;
                Some(SinkEvent::End)
            }
            Ok(e) => Some(e),
            Err(e @ mpsc::RecvError) => {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    e,
                )));
            }
        })
    }

    fn feed_event(&mut self, event: SourceEvent) -> Result<(), Error> {
        match event {
            SourceEvent::NewFile(name, modified) => self.new_file(&name, modified),
            SourceEvent::NewBlock(hash, size) => self.new_block(&hash, size),
            SourceEvent::End => {
                self.writer.write_all(b"END_FILES\n")?;
                Ok(())
            }
            SourceEvent::BlockData(hash, block) => self.feed_block(&hash, &block),
        }
    }

    fn is_missing_blocks(&self) -> Result<bool, Error> {
        Ok(!self.done)
    }
}

/// Decode stream from the remote sink, parsing block requests
fn recv_from_sink<R: Read>(
    mut reader: R,
    tx: mpsc::SyncSender<SinkEvent>,
) {
    let mut reader = SyncReader::new(|buf| {
        let n = reader.read(buf)?;
        info!("recv_from_sink: {:?}", n);
        if n == 0 {
            Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "End-of-file",
            ))
        } else {
            Ok(n)
        }
    });
    let res: Result<(), CommunicationError<std::io::Error>> = (move || {
        loop {
            let cmd = reader.read_to_space()?;
            if &reader[cmd.clone()] == b"REQBLOCK" {
                let hash = reader.read_str()?;
                reader.read_eol()?;

                // Parse hash
                let hash: HashDigest = std::str::from_utf8(&reader[hash])
                    .ok().and_then(|s| HashDigest::from_hex(s).ok())
                    .ok_or(CommunicationError::ProtocolError(
                        "Invalid hash",
                    ))?;

                info!("Got block request from sink");
                tx.send(SinkEvent::BlockRequest(hash)).unwrap();
            } else if &reader[cmd] == b"END" {
                reader.read_eol()?;

                info!("Got end from sink");
                tx.send(SinkEvent::End).unwrap();
                return Ok(());
            } else {
                return Err(CommunicationError::ProtocolError(
                    "Invalid command",
                ));
            }
            reader.end();
        }
    })();
    if let Err(e) = res {
        error!("Error reading from destination: {}", e);
    }
}

impl SinkWrapper for SshWrapper {
    fn open(&mut self) -> Result<Box<dyn Sink>, Error> {
        let mut child = run_ssh(&self.0, &["piped-sink", &self.0.path])?;
        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take().unwrap();
        let stdin = child.stdin.take().unwrap();
        let (tx, rx) = mpsc::sync_channel(1);
        thread::spawn(move || recv_errors(stderr, "sink"));
        thread::spawn(move || recv_from_sink(stdout, tx));
        Ok(Box::new(SshSink {
            child: Some(child),
            writer: stdin,
            rx,
            done: false,
        }))
    }
}

/// Source reading from a remote machine via SSH
pub struct SshSource<W: Write> {
    child: Option<Child>,
    writer: W,
    rx: mpsc::Receiver<SourceEvent>,
}

impl<W: Write> Drop for SshSource<W> {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            // Join SSH process
            match child.wait() {
                Ok(s) => {
                    if !s.success() {
                        error!("SSH to source exited with {:?}", s);
                    }
                }
                Err(e) => {
                    error!("Error waiting on SSH process to source: {}", e);
                }
            }
        }
    }
}

impl<W: Write> SshSource<W> {
    pub fn piped<R>(stdin: W, stdout: R) -> SshSource<W>
        where R: Read + Send + 'static
    {
        let (tx, rx) = mpsc::sync_channel(1);
        thread::spawn(move || recv_from_source(stdout, tx));
        SshSource {
            child: None,
            writer: stdin,
            rx,
        }
    }
}

impl<W: Write> Source for SshSource<W> {
    fn next_event(&mut self) -> Result<SourceEvent, Error> {
        let event = match self.rx.recv() {
            Ok(event) => event,
            Err(e @ mpsc::RecvError) => {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    e,
                )));
            }
        };
        Ok(event)
    }

    fn feed_event(&mut self, event: SinkEvent) -> Result<(), Error> {
        match event {
            SinkEvent::BlockRequest(hash) => {
                writeln!(self.writer, "REQBLOCK 40:{}", hash)?;
            }
            SinkEvent::End => {
                self.writer.write_all(b"END\n")?;
            }
        }
        Ok(())
    }
}

/// Decode stream from the remote source, parsing instructions and blocks
fn recv_from_source<R: Read>(
    mut reader: R,
    tx: mpsc::SyncSender<SourceEvent>,
) {
    let mut reader = SyncReader::new(|buf| {
        let n = reader.read(buf)?;
        info!("recv_from_source: {:?}", n);
        //info!("{}", unsafe { std::str::from_utf8_unchecked(&buf[.. n]) });
        if n == 0 {
            Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "End-of-file",
            ))
        } else {
            Ok(n)
        }
    });
    let res: Result<(), CommunicationError<std::io::Error>> = (move || {
        loop {
            let cmd = reader.read_to_space()?;
            info!("Reading {} from source", String::from_utf8_lossy(&reader[cmd.clone()]));
            if &reader[cmd.clone()] == b"FILE" {
                let name = reader.read_str()?;
                reader.read_space()?;
                let modified = reader.read_to_eol()?;

                let name = path_from_u8(&reader[name]);

                // Parse datetime
                let modified: i64 = std::str::from_utf8(&reader[modified])
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .ok_or(CommunicationError::ProtocolError(
                        "Invalid timestamp",
                    ))?;
                let modified = chrono::DateTime::<chrono::Utc>::from_utc(
                    chrono::NaiveDateTime::from_timestamp(modified, 0),
                    chrono::Utc,
                );

                let event = SourceEvent::NewFile(
                    name.into_owned(),
                    modified,
                );
                info!("Got file from source");
                tx.send(event).unwrap();
            } else if &reader[cmd.clone()] == b"BLOCK" {
                let hash = reader.read_str()?;
                reader.read_space()?;
                let size = reader.read_to_eol()?;

                // Parse hash
                let hash: HashDigest = std::str::from_utf8(&reader[hash])
                    .ok().and_then(|s| HashDigest::from_hex(s).ok())
                    .ok_or(CommunicationError::ProtocolError(
                        "Invalid hash",
                    ))?;

                // Parse size
                let size = std::str::from_utf8(&reader[size])
                    .ok().and_then(|s| s.parse().ok())
                    .ok_or(CommunicationError::ProtocolError(
                        "Invalid size",
                    ))?;

                info!("Got block from source");
                let event = SourceEvent::NewBlock(hash, size);
                tx.send(event).unwrap();
            } else if &reader[cmd.clone()] == b"END_FILES" {
                reader.read_eol()?;

                info!("Got end from source");
                tx.send(SourceEvent::End).unwrap();
            } else if &reader[cmd] == b"DATA" {
                let hash = reader.read_str()?;
                reader.read_space()?;
                let block = reader.read_block()?;
                reader.read_eol()?;

                // Parse hash
                let hash: HashDigest = std::str::from_utf8(&reader[hash])
                    .ok().and_then(|s| HashDigest::from_hex(s).ok())
                    .ok_or(CommunicationError::ProtocolError(
                        "Invalid hash",
                    ))?;

                let event = SourceEvent::BlockData(hash, block);
                info!("Got data from source");
                tx.send(event).unwrap();
            } else {
                return Err(CommunicationError::ProtocolError(
                    "Invalid command",
                ));
            }
            reader.end();
        }
    })();
    if let Err(e) = res {
        error!("Error reading from source: {}", e);
    }
}

impl SourceWrapper for SshWrapper {
    fn open(&mut self) -> Result<Box<dyn Source>, Error> {
        let mut child = run_ssh(&self.0, &["piped-source", &self.0.path])?;
        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take().unwrap();
        let stdin = child.stdin.take().unwrap();
        let (tx, rx) = mpsc::sync_channel(1);
        thread::spawn(move || recv_errors(stderr, "source"));
        thread::spawn(move || recv_from_source(stdout, tx));
        Ok(Box::new(SshSource {
            child: Some(child),
            writer: stdin,
            rx,
        }))
    }
}
