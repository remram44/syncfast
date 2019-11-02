use std::borrow::Cow;
use std::io::{BufRead, BufReader, Write};
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
        eprintln!("{},  error reading stderr: {}", prefix, e);
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
                    eprintln!("SSH to destination exited with {:?}", s);
                }
            }
            Err(e) => {
                eprintln!(
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

/// Decode stream from the remote sink, parsing block requests
fn recv_from_sink(
    stdout: ChildStdout,
    tx: mpsc::SyncSender<Option<HashDigest>>,
) {
    // TODO: Read from sink stdout, parse block requests
    unimplemented!()
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
                    eprintln!("SSH to source exited with {:?}", s);
                }
            }
            Err(e) => {
                eprintln!("Error waiting on SSH process to source: {}", e);
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
    stdout: ChildStdout,
    index_tx: mpsc::Sender<IndexEvent>,
    blocks_tx: mpsc::SyncSender<(HashDigest, Vec<u8>)>,
) {
    // TODO: Read from source stdout, parse instructions and blocks
    unimplemented!()
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
