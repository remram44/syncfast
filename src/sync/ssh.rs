use std::borrow::Cow;
use std::io::{BufRead, BufReader, Read, Write};
use std::ops::{Deref, Range};
use std::path::Path;
use std::process::{Child, ChildStderr, ChildStdout, Command, Stdio};
use std::sync::mpsc;
use std::thread;

use crate::{Error, HashDigest};
use crate::locations::SshLocation;
use crate::sync::{IndexEvent, Sink, SinkWrapper, Source, SourceWrapper};

/// The wrapper for SSH endpoints
pub struct SshWrapper(pub SshLocation);

/// Run an SSH command with stdio piped and the given destination and args
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

/// Read from stderr, print it here with a prefix
fn recv_errors(stderr: ChildStderr, prefix: &'static str) {
    let mut stderr = BufReader::new(stderr);
    let mut buffer = String::new();
    let r: std::io::Result<()> = (|| {
        while stderr.read_line(&mut buffer)? > 0 {
            eprintln!("remote {}: {}", prefix, buffer);
        }
        Ok(())
    })();
    if let Err(e) = r {
        error!("{},  error reading stderr: {}", prefix, e);
    }
}

/// Sink writing to a remote machine via SSH
pub struct SshSink {
    child: Child,
    block_reqs_rx: mpsc::Receiver<Option<HashDigest>>,
    done: bool,
}

impl Drop for SshSink {
    fn drop(&mut self) {
        // Join SSH process
        match self.child.wait() {
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

#[cfg(unix)]
fn path_to_u8(path: &Path) -> Cow<[u8]> {
    use std::os::unix::ffi::OsStrExt;
    Cow::Borrowed(path.as_os_str().as_bytes())
}

#[cfg(not(unix))]
fn path_to_u8(path: &Path) -> Cow<[u8]> {
    match path.as_os_str().to_string_lossy() {
        Cow::Borrowed(s) => Cow::Borrowed(s.as_bytes()),
        Cow::Owned(s) => Cow::Owned(s.into_bytes()),
    }
}

impl Sink for SshSink {
    fn new_file(
        &mut self,
        name: &Path,
        modified: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), Error> {
        let stdin = self.child.stdin.as_mut().unwrap();
        let path = path_to_u8(name);
        write!(stdin, "FILE {}:", path.len())?;
        stdin.write_all(&path_to_u8(name))?;
        writeln!(stdin, " {}", modified.timestamp())?;
        Ok(())
    }

    fn new_block(
        &mut self,
        hash: &HashDigest,
        size: usize,
    ) -> Result<(), Error> {
        let stdin = self.child.stdin.as_mut().unwrap();
        writeln!(stdin, "BLOCK 40:{} {}", hash, size)?;
        Ok(())
    }

    fn end_files(&mut self) -> Result<(), Error> {
        let stdin = self.child.stdin.as_mut().unwrap();
        stdin.write_all(b"END_FILES\n")?;
        Ok(())
    }

    fn feed_block(
        &mut self,
        hash: &HashDigest,
        block: &[u8],
    ) -> Result<(), Error> {
        let stdin = self.child.stdin.as_mut().unwrap();
        write!(stdin, "DATA 40:{} {}:", hash, block.len())?;
        stdin.write_all(block)?;
        stdin.write_all(b"\n")?;
        Ok(())
    }

    fn next_requested_block(&mut self) -> Result<Option<HashDigest>, Error> {
        let hash = match self.block_reqs_rx.try_recv() {
            Ok(Some(hash)) => Some(hash),
            Ok(None) => {
                self.done = true;
                None
            }
            Err(mpsc::TryRecvError::Empty) => None,
            Err(e @ mpsc::TryRecvError::Disconnected) => {
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

struct SyncReader<R: Read> {
    /// Wrapped reader
    reader: R,
    buffer: [u8; 4096],
    /// How much we have consumed of the buffer
    pos: usize,
    /// How many bytes we read to the buffer
    size: usize,
}

impl<R: Read> SyncReader<R> {
    fn new(reader: R) -> SyncReader<R> {
        SyncReader { reader, buffer: [0u8; 4096], pos: 0, size: 0 }
    }

    /// Read some more bytes
    fn read(&mut self) -> std::io::Result<usize> {
        let bytes = self.reader.read(&mut self.buffer[self.size ..])?;
        self.size += bytes;
        Ok(bytes)
    }

    /// Read more bytes we need
    fn read_at_least(&mut self, bytes: usize) -> std::io::Result<()> {
        let target = self.size + bytes;
        if target > 4096 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Command too long",
            ));
        }
        while self.size < target {
            self.read()?;
        }
        Ok(())
    }

    /// Read until the next space
    fn read_to_space(&mut self) -> std::io::Result<Range<usize>> {
        let mut prev_pos = self.pos; // No space until here
        loop {
            // Find a space
            if let Some(space_idx) = self.buffer[prev_pos .. self.size]
                .iter()
                .position(|&b| b == b' ')
            {
                let space_idx = prev_pos + space_idx;
                let slice = self.pos .. space_idx;
                self.pos = space_idx + 1;
                // Return slice
                return Ok(slice);
            } else {
                prev_pos = self.size;
            }

            // Read more bytes
            self.read()?;
        }
    }

    /// Read a string prefixed by its length and a colon
    fn read_str(&mut self) -> std::io::Result<Range<usize>> {
        let mut prev_pos = self.pos; // No colon until here
        loop {
            // Find a colon
            if let Some(colon_idx) = self.buffer[prev_pos .. self.size]
                .iter()
                .position(|&b| b == b' ')
            {
                // Get the size
                let colon_idx = prev_pos + colon_idx;
                let size = &self.buffer[self.pos .. colon_idx];

                // Parse it to a number
                let size: Option<usize> = std::str::from_utf8(size)
                    .ok()
                    .and_then(|s| s.parse().ok());
                let size = match size {
                    Some(i) => i,
                    None => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            "Invalid string size",
                        ));
                    }
                };

                // Read the string
                if colon_idx + 1 + size > self.size {
                    self.read_at_least(colon_idx + 1 + size - self.size)?;
                }

                // Return slice
                return Ok(colon_idx + 1 .. colon_idx + 1 + size);
            } else {
                prev_pos = self.size;
            }
        }
    }

    /// Consume a space
    fn read_space(&mut self) -> std::io::Result<()> {
        if self.pos + 1 <= self.size {
            self.read_at_least(1)?;
        }
        if self.buffer[self.pos] != b' ' {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Missing space",
            ));
        }
        self.pos += 1;
        Ok(())
    }

    /// Consume a line ending and clear what was consumed from the buffer
    fn end(&mut self) -> std::io::Result<()> {
        // Line ending
        if self.pos + 1 <= self.size {
            self.read_at_least(1)?;
        }
        if self.buffer[self.pos] != b'\n' {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Missing line ending",
            ));
        }
        self.pos += 1;

        // Discard what was consumed
        self.buffer.copy_within(self.pos .. self.size, 0);
        self.size -= self.pos;
        self.pos = 0;
        Ok(())
    }
}

impl<R: Read> Deref for SyncReader<R> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.buffer[0 .. self.size]
    }
}

/// Decode stream from the remote sink, parsing block requests
fn recv_from_sink(
    stdout: ChildStdout,
    tx: mpsc::SyncSender<Option<HashDigest>>,
) {
    let mut reader = SyncReader::new(stdout);
    let res: std::io::Result<()> = (move || {
        loop {
            let cmd = reader.read_to_space()?;
            if &reader[cmd.clone()] == b"REQBLOCK" {
                let hash = reader.read_str()?;
                reader.end()?;

                let hash: HashDigest = std::str::from_utf8(&reader[hash])
                    .ok().and_then(|s| HashDigest::from_hex(s).ok())
                    .ok_or(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Missing space",
                    ))?;
                tx.send(Some(hash)).unwrap();
            } else if &reader[cmd] == b"END" {
                reader.end()?;

                tx.send(None).unwrap();
                return Ok(());
            } else {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Invalid command",
                ));
            }
        }
    })();
    if let Err(e) = res {
        error!("Error reading from destination: {}", e);
    }
}

impl SinkWrapper for SshWrapper {
    fn open(&mut self) -> Result<Box<dyn Sink>, Error> {
        let mut child = run_ssh(&self.0, &["piped-sink"])?;
        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take().unwrap();
        let (block_reqs_tx, block_reqs_rx) = mpsc::sync_channel(1);
        thread::spawn(move || recv_errors(stderr, "sink"));
        thread::spawn(move || recv_from_sink(stdout, block_reqs_tx));
        Ok(Box::new(SshSink {
            child,
            block_reqs_rx,
            done: false,
        }))
    }
}

/// Source reading from a remote machine via SSH
pub struct SshSource {
    child: Child,
    index_rx: mpsc::Receiver<IndexEvent>,
    blocks_rx: mpsc::Receiver<(HashDigest, Vec<u8>)>,
}

impl Drop for SshSource {
    fn drop(&mut self) {
        // Join SSH process
        match self.child.wait() {
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

impl Source for SshSource {
    fn next_from_index(&mut self) -> Result<Option<IndexEvent>, Error> {
        let event = match self.index_rx.try_recv() {
            Ok(event) => Some(event),
            Err(mpsc::TryRecvError::Empty) => None,
            Err(e @ mpsc::TryRecvError::Disconnected) => {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    e,
                )));
            }
        };
        Ok(event)
    }

    fn request_block(&mut self, hash: &HashDigest) -> Result<(), Error> {
        let stdin = self.child.stdin.as_mut().unwrap();
        writeln!(stdin, "REQBLOCK 40:{}", hash)?;
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
        let stdin = self.child.stdin.as_mut().unwrap();
        stdin.write_all(b"END\n")?;
        Ok(())
    }
}

/// Decode stream from the remote source, parsing instructions and blocks
fn recv_from_source(
    mut stdout: ChildStdout,
    index_tx: mpsc::Sender<IndexEvent>,
    blocks_tx: mpsc::SyncSender<(HashDigest, Vec<u8>)>,
) {
    unimplemented!() // TODO
}

impl SourceWrapper for SshWrapper {
    fn open(&mut self) -> Result<Box<dyn Source>, Error> {
        let mut child = run_ssh(&self.0, &["piped-source"])?;
        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take().unwrap();
        let (index_tx, index_rx) = mpsc::channel();
        let (blocks_tx, blocks_rx) = mpsc::sync_channel(1);
        thread::spawn(move || recv_errors(stderr, "source"));
        thread::spawn(move || recv_from_source(stdout, index_tx, blocks_tx));
        Ok(Box::new(SshSource {
            child,
            index_rx,
            blocks_rx,
        }))
    }
}
