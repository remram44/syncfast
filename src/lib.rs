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

pub mod locations;
mod index;

use rusqlite::types::{FromSql, FromSqlError, ToSql, ToSqlOutput};
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

#[derive(Debug)]
enum InvalidHashDigest {
    WrongSize,
    InvalidChar,
}

impl fmt::Display for InvalidHashDigest {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            InvalidHashDigest::WrongSize => {
                write!(f, "Invalid hash: wrong size")
            }
            InvalidHashDigest::InvalidChar => {
                write!(f, "Invalid hash: invalid character")
            }
        }
    }
}

impl std::error::Error for InvalidHashDigest {}

impl FromSql for HashDigest {
    fn column_result(
        value: rusqlite::types::ValueRef,
    ) -> Result<HashDigest, FromSqlError>
    {
        value
            .as_str()
            .and_then(|s| {
                if s.len() != 40 {
                    Err(FromSqlError::Other(Box::new(
                        InvalidHashDigest::WrongSize
                    )))
                } else {
                    let mut bytes = [0u8; 20];
                    for (i, byte) in (&mut bytes).iter_mut().enumerate() {
                        *byte = u8::from_str_radix(&s[i*2 .. i*2 + 2], 16)
                            .map_err(|_| FromSqlError::Other(Box::new(
                                InvalidHashDigest::InvalidChar
                            )))?;
                    }
                    Ok(HashDigest(bytes))
                }
            })
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
    use rusqlite::types::{FromSql, ToSql, ToSqlOutput, Value, ValueRef};
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

    #[test]
    fn test_hash_fromsql() {
        let mut sha1 = Sha1::new();
        sha1.update(b"test");
        let digest = HashDigest(sha1.digest().bytes());

        let hash = <HashDigest as FromSql>::column_result(ValueRef::Text(
            "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3"
        ));
        assert_eq!(
            hash.unwrap(),
            digest,
        );
    }
}
