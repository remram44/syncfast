//! Rsync-like library/program.
//!
//! Rrsync is intended to provide the functionality of rsync ("live" transfer
//! of files/directories over SSH), rdiff (creation of binary patches between
//! files, for later application), and zsync (efficient synchronization of
//! files or file trees from a central "dumb" HTTP server). It also has some
//! additions such as caching file signatures to make repeated synchronizations
//! faster.

extern crate cdchunking;
extern crate chrono;
#[macro_use] extern crate log;
extern crate rusqlite;
extern crate sha1;
#[cfg(test)] extern crate tempfile;

mod index;

use rusqlite::types::{ToSql, ToSqlOutput};
use std::fmt;
use std::io::Write;

pub use index::{Index, IndexTransaction};

/// General error type for this library
#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    Sqlite(rusqlite::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Sqlite(e) => write!(f, "SQLite error: {}", e),
            Error::Io(e) => write!(f, "I/O error: {}", e),
        }
    }
}

impl std::error::Error for Error {}

impl From<rusqlite::Error> for Error {
    fn from(e: rusqlite::Error) -> Error {
        Error::Sqlite(e)
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Error {
        Error::Io(e)
    }
}

/// Type for the hashes
#[derive(Debug, PartialEq, Eq)]
pub struct HashDigest([u8; 20]);

impl ToSql for HashDigest {
    fn to_sql(&self) -> Result<ToSqlOutput, rusqlite::Error> {
        // Write the hash to buffer on the stack, we know the size
        let mut buffer = Vec::with_capacity(40);
        for byte in &self.0 {
            write!(&mut buffer, "{:02x}", byte).unwrap();
        }
        // Hexadecimal chars are ASCII, cast to string
        let string = String::from_utf8(buffer).unwrap();

        Ok(ToSqlOutput::from(string))
    }
}

impl fmt::Display for HashDigest {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for byte in &self.0 {
            write!(f, "{:02x}", byte)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::types::{ToSql, ToSqlOutput, Value};
    use sha1::Sha1;

    use super::HashDigest;

    #[test]
    fn test_hash_tosql() {
        let mut sha1 = Sha1::new();
        sha1.update(b"test");
        let digest = HashDigest(sha1.digest().bytes());
        assert_eq!(
            digest.to_sql().unwrap(),
            ToSqlOutput::Owned(
                Value::Text("a94a8fe5ccb19ba61c4c0873d391e987982fbbd3".into())
            ),
        );
    }
}
