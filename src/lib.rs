extern crate chrono;
#[macro_use] extern crate log;
extern crate rusqlite;

use rusqlite::Connection;
use rusqlite::types::{ToSql, ToSqlOutput};
use std::fmt;
use std::path::{Path, PathBuf};

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

#[derive(Debug, PartialEq, Eq)]
pub struct HashDigest([u8; 20]);

impl ToSql for HashDigest {
    fn to_sql(&self) -> Result<ToSqlOutput, rusqlite::Error> {
        unimplemented!()
    }
}

const SCHEMA: &'static str = "
    CREATE TABLE version(
        name VARCHAR(8) NOT NULL,
        version VARCHAR(16) NOT NULL
    );
    INSERT INTO version(name, version) VALUES('rs-sync', '0.1');

    CREATE TABLE files(
        file_id INTEGER NOT NULL PRIMARY KEY,
        name VARCHAR(512) NOT NULL,
        modified DATETIME NOT NULL
    );
    CREATE INDEX idx_files_name ON files(name);

    CREATE TABLE blocks(
        hash VARCHAR(40) NOT NULL,
        file_id INTEGER NOT NULL,
        offset INTEGER NOT NULL,
        PRIMARY KEY(file_id, offset)
    );
    CREATE INDEX idx_blocks_hash ON blocks(hash);
    CREATE INDEX idx_blocks_file ON blocks(file_id);
    CREATE INDEX idx_blocks_file_offset ON blocks(file_id, offset);
";

pub struct Index {
    db: Connection,
}

impl Index {
    pub fn open(filename: &Path) -> Result<Index, Error> {
        let exists = filename.exists();
        let db = Connection::open(filename)?;
        if !exists {
            warn!("Database doesn't exist, creating tables...");
            db.execute_batch(SCHEMA)?;
        }
        Ok(Index { db })
    }

    pub fn add_file(
        &mut self,
        name: &Path,
        modified: chrono::NaiveDateTime,
    ) -> Result<u32, Error>
    {
        let modified = modified.format("%Y-%m-%d %H:%M:%S").to_string();
        let mut stmt = self.db.prepare(
            "
            SELECT file_id FROM files
            WHERE name = ?;
            ",
        )?;
        let mut rows = stmt.query(&[name.to_str().expect("encoding")])?;
        if let Some(row) = rows.next() {
            let file_id: u32 = row?.get(0);
            self.db.execute(
                "
                UPDATE files SET modified = ? WHERE file_id = ?;
                ",
                &[&file_id as &dyn ToSql, &modified],
            )?;
            Ok(file_id)
        } else {
            self.db.execute(
                "
                INSERT INTO files(name, modified)
                VALUES(?, ?);
                ",
                &[name.to_str().expect("encoding"), &modified],
            )?;
            let file_id = self.db.last_insert_rowid();
            Ok(file_id as u32)
        }
    }

    pub fn remove_file(
        &mut self,
        name: &Path,
    ) -> Result<(), Error>
    {
        self.db.execute(
            "
            DELETE FROM blocks WHERE file_id = (
                SELECT file_id FROM files
                WHERE name = ?
            );
            ",
            &[name.to_str().expect("encoding")],
        )?;
        self.db.execute(
            "
            DELETE FROM files WHERE name = ?;
            ",
            &[name.to_str().expect("encoding")],
        )?;
        Ok(())
    }

    pub fn add_block(
        &mut self,
        hash: HashDigest,
        file_id: u32,
        offset: usize,
    ) -> Result<(), Error>
    {
        self.db.execute(
            "
            INSERT INTO blocks(hash, file_id, offset)
            VALUES(?, ?, ?);
            ",
            &[&hash as &dyn ToSql, &file_id, &(offset as i64)],
        )?;
        Ok(())
    }

    pub fn get_block(
        &self,
        hash: HashDigest,
    ) -> Result<Option<(PathBuf, usize)>, Error>
    {
        let mut stmt = self.db.prepare(
            "
            SELECT blocks.file_id, blocks.offset
            FROM blocks
            INNER JOIN files ON blocks.file_id = files.file_id
            WHERE blocks.hash = ?;
            ",
        )?;
        let mut rows = stmt.query(&[&hash as &dyn ToSql])?;
        if let Some(row) = rows.next() {
            let row = row?;
            let path: String = row.get(0);
            let path: PathBuf = path.into();
            let offset: i64 = row.get(1);
            let offset = offset as usize;
            Ok(Some((path, offset)))
        } else {
            Ok(None)
        }
    }
}
