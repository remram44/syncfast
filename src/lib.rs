extern crate chrono;
#[macro_use] extern crate log;
extern crate rusqlite;

mod index;

use rusqlite::types::{ToSql, ToSqlOutput};
use std::fmt;

pub use index::Index;

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
        unimplemented!()
    }
}
