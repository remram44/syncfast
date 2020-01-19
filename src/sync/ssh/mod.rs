mod proto;

use std::io::{BufRead, BufReader, Read, Write};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::thread;

use crate::{Error, HashDigest};
use crate::locations::SshLocation;
use crate::sync::{IndexEvent, Sink, SinkWrapper, Source, SourceWrapper};
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
    block_reqs_rx: mpsc::Receiver<Option<HashDigest>>,
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
        let (block_reqs_tx, block_reqs_rx) = mpsc::sync_channel(1);
        thread::spawn(move || recv_from_sink(stdout, block_reqs_tx));
        SshSink {
            child: None,
            writer: stdin,
            block_reqs_rx,
            done: false,
        }
    }
}

impl<W: Write> Sink for SshSink<W> {
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

    fn end_files(&mut self) -> Result<(), Error> {
        self.writer.write_all(b"END_FILES\n")?;
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

    fn next_requested_block(&mut self) -> Result<Option<HashDigest>, Error> {
        let hash = match self.block_reqs_rx.recv() {
            Ok(Some(hash)) => Some(hash),
            Ok(None) => {
                self.done = true;
                None
            }
            Err(e @ mpsc::RecvError) => {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    e,
                )));
            }
        };
        Ok(hash)
    }

    fn is_missing_blocks(&self) -> Result<bool, Error> {
        Ok(!self.done)
    }
}

/// Decode stream from the remote sink, parsing block requests
fn recv_from_sink<R: Read>(
    mut reader: R,
    tx: mpsc::SyncSender<Option<HashDigest>>,
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
                tx.send(Some(hash)).unwrap();
            } else if &reader[cmd] == b"END" {
                reader.read_eol()?;

                info!("Got end from sink");
                tx.send(None).unwrap();
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
        let (block_reqs_tx, block_reqs_rx) = mpsc::sync_channel(1);
        thread::spawn(move || recv_errors(stderr, "sink"));
        thread::spawn(move || recv_from_sink(stdout, block_reqs_tx));
        Ok(Box::new(SshSink {
            child: Some(child),
            writer: stdin,
            block_reqs_rx,
            done: false,
        }))
    }
}

/// Source reading from a remote machine via SSH
pub struct SshSource<W: Write> {
    child: Option<Child>,
    writer: W,
    index_rx: mpsc::Receiver<IndexEvent>,
    blocks_rx: mpsc::Receiver<(HashDigest, Vec<u8>)>,
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
        let (index_tx, index_rx) = mpsc::channel();
        let (blocks_tx, blocks_rx) = mpsc::sync_channel(1);
        thread::spawn(move || recv_from_source(stdout, index_tx, blocks_tx));
        SshSource {
            child: None,
            writer: stdin,
            index_rx,
            blocks_rx,
        }
    }
}

impl<W: Write> Source for SshSource<W> {
    fn next_from_index(&mut self) -> Result<IndexEvent, Error> {
        let event = match self.index_rx.recv() {
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

    fn request_block(&mut self, hash: &HashDigest) -> Result<(), Error> {
        writeln!(self.writer, "REQBLOCK 40:{}", hash)?;
        Ok(())
    }

    fn get_next_block(
        &mut self,
    ) -> Result<Option<(HashDigest, Vec<u8>)>, Error> {
        let res = match self.blocks_rx.recv() {
            Ok(r) => Some(r),
            Err(e @ mpsc::RecvError) => {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    e,
                )));
            }
        };
        Ok(res)
    }

    fn end(&mut self) -> Result<(), Error> {
        self.writer.write_all(b"END\n")?;
        Ok(())
    }
}

/// Decode stream from the remote source, parsing instructions and blocks
fn recv_from_source<R: Read>(
    mut reader: R,
    index_tx: mpsc::Sender<IndexEvent>,
    blocks_tx: mpsc::SyncSender<(HashDigest, Vec<u8>)>,
) {
    let mut reader = SyncReader::new(|buf| {
        let n = reader.read(buf)?;
        info!("recv_from_source: {:?}", n);
        info!("{}", unsafe { std::str::from_utf8_unchecked(buf) });
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

                let event = IndexEvent::NewFile(
                    name.into_owned(),
                    modified,
                );
                info!("Got file from source");
                index_tx.send(event).unwrap();
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
                let event = IndexEvent::NewBlock(hash, size);
                index_tx.send(event).unwrap();
            } else if &reader[cmd.clone()] == b"END_FILES" {
                reader.read_eol()?;

                info!("Got end from source");
                index_tx.send(IndexEvent::End).unwrap();
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

                info!("Got data from source");
                blocks_tx.send((hash, block)).unwrap();
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
        let (index_tx, index_rx) = mpsc::channel();
        let (blocks_tx, blocks_rx) = mpsc::sync_channel(1);
        thread::spawn(move || recv_errors(stderr, "source"));
        thread::spawn(move || recv_from_source(stdout, index_tx, blocks_tx));
        Ok(Box::new(SshSource {
            child: Some(child),
            writer: stdin,
            index_rx,
            blocks_rx,
        }))
    }
}
