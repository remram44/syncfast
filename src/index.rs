use cdchunking::{Chunker, ChunkInput, ZPAQ};
use rusqlite;
use rusqlite::{Connection, Transaction};
use rusqlite::types::ToSql;
use sha1::Sha1;
use std::fs::File;
use std::path::{Path, PathBuf};

use crate::{Error, HashDigest};

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

/// Index of files and blocks
pub struct Index {
    db: Connection,
}

impl Index {
    /// Open an index from a file
    pub fn open(filename: &Path) -> Result<Index, Error> {
        let exists = filename.exists();
        let db = Connection::open(filename)?;
        if !exists {
            warn!("Database doesn't exist, creating tables...");
            db.execute_batch(SCHEMA)?;
        }
        Ok(Index { db })
    }

    /// Open an in-memory index
    pub fn open_in_memory() -> Result<Index, Error> {
        let db = Connection::open_in_memory()?;
        db.execute_batch(SCHEMA)?;
        Ok(Index { db })
    }

    /// Try to find a block in the indexed files
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

    /// Start a transaction to update the index
    pub fn transaction<'a>(
        &'a mut self
    ) -> Result<IndexTransaction<'a>, rusqlite::Error>
    {
        let tx = self.db.transaction()?;
        Ok(IndexTransaction { tx })
    }
}

pub struct IndexTransaction<'a> {
    tx: Transaction<'a>,
}

impl<'a> IndexTransaction<'a> {
    /// Add a file to the index
    ///
    /// This returns a tuple `(file_id, up_to_date)` where `file_id` can be
    /// used to insert blocks, and `up_to_date` indicates whether the file's
    /// modification date has changed and it should be re-indexed.
    pub fn add_file(
        &mut self,
        name: &Path,
        modified: chrono::DateTime<chrono::Utc>,
    ) -> Result<(u32, bool), Error>
    {
        let mut stmt = self.tx.prepare(
            "
            SELECT file_id, modified FROM files
            WHERE name = ?;
            ",
        )?;
        let mut rows = stmt.query(&[name.to_str().expect("encoding")])?;
        if let Some(row) = rows.next() {
            let row = row?;
            let file_id: u32 = row.get(0);
            let old_modified: chrono::DateTime<chrono::Utc> = row.get(1);
            if old_modified != modified {
                info!("Resetting file {:?}, modified", name);
                // Delete blocks
                self.tx.execute(
                    "
                    DELETE FROM blocks WHERE file_id = ?;
                    ",
                    &[&file_id],
                )?;
                // Update modification time
                self.tx.execute(
                    "
                    UPDATE files SET modified = ? WHERE file_id = ?;
                    ",
                    &[&modified as &dyn ToSql, &file_id],
                )?;
                Ok((file_id, false))
            } else {
                debug!("File {:?} up to date", name);
                Ok((file_id, true))
            }
        } else {
            info!("Inserting new file {:?}", name);
            self.tx.execute(
                "
                INSERT INTO files(name, modified)
                VALUES(?, ?);
                ",
                &[&name.to_str().expect("encoding") as &dyn ToSql, &modified],
            )?;
            let file_id = self.tx.last_insert_rowid();
            Ok((file_id as u32, false))
        }
    }

    /// Remove a file and all its blocks from the index
    pub fn remove_file(
        &mut self,
        file_id: u32,
    ) -> Result<(), Error>
    {
        self.tx.execute(
            "
            DELETE FROM blocks WHERE file_id = ?;
            ",
            &[&file_id],
        )?;
        self.tx.execute(
            "
            DELETE FROM files WHERE file_id = ?;
            ",
            &[&file_id],
        )?;
        Ok(())
    }

    /// Get a list of all the files in the index
    pub fn list_files(&self) -> Result<Vec<(u32, PathBuf)>, Error> {
        let mut stmt = self.tx.prepare(
            "
            SELECT file_id, name FROM files;
            ",
        )?;
        let mut rows = stmt.query(rusqlite::NO_PARAMS)?;
        let mut results = Vec::new();
        loop {
            match rows.next() {
                Some(Ok(row)) => {
                    let path: String = row.get(1);
                    results.push((row.get(0), path.into()))
                }
                Some(Err(e)) => return Err(e.into()),
                None => break,
            }
        }
        Ok(results)
    }

    /// Add a block to the index
    pub fn add_block(
        &mut self,
        hash: HashDigest,
        file_id: u32,
        offset: usize,
    ) -> Result<(), Error>
    {
        self.tx.execute(
            "
            INSERT INTO blocks(hash, file_id, offset)
            VALUES(?, ?, ?);
            ",
            &[&hash as &dyn ToSql, &file_id, &(offset as i64)],
        )?;
        Ok(())
    }

    /// Cut up a file into blocks and add them to the index
    pub fn index_file(
        &mut self,
        name: &Path,
    ) -> Result<(), Error>
    {
        let file = File::open(name)?;
        let (file_id, up_to_date) = self.add_file(
            name,
            file.metadata()?.modified()?.into(),
        )?;
        if !up_to_date {
            // Use ZPAQ to cut the stream into blocks
            let chunker = Chunker::new(
                ZPAQ::new(13) // 13 bits = 8 KiB block average
            );
            let mut chunk_iterator = chunker.stream(file);
            let mut start_offset = 0;
            let mut offset = 0;
            let mut sha1 = Sha1::new();
            while let Some(chunk) = chunk_iterator.read() {
                match chunk? {
                    ChunkInput::Data(d) => {
                        sha1.update(d);
                        offset += d.len();
                    }
                    ChunkInput::End => {
                        let digest = HashDigest(sha1.digest().bytes());
                        debug!(
                            "Adding block, offset={}, size={}, sha1={}",
                            start_offset, offset - start_offset, sha1.digest(),
                        );
                        self.add_block(digest, file_id, start_offset)?;
                        start_offset = offset;
                        sha1.reset();
                    }
                }
            }
        }
        Ok(())
    }

    /// Commit the transaction
    pub fn commit(self) -> Result<(), rusqlite::Error> {
        self.tx.commit()
    }
}
