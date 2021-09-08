use cdchunking::{ChunkInput, Chunker, ZPAQ};
use log::{debug, info, warn};
use rusqlite;
use rusqlite::Connection;
use rusqlite::types::ToSql;
use sha1::Sha1;
use std::fs::File;
use std::path::{Path, PathBuf};

use crate::{Error, HashDigest};

const SCHEMA: &str = "
    CREATE TABLE files(
        file_id INTEGER NOT NULL PRIMARY KEY,
        name VARCHAR(512) NOT NULL,
        modified DATETIME NOT NULL,
        size INTEGER NULL,
        blocks_hash VARCHAR(40) NULL
    );
    CREATE INDEX idx_files_name ON files(name);

    CREATE TABLE blocks(
        file_id INTEGER NOT NULL,
        hash VARCHAR(40) NOT NULL,
        offset INTEGER NOT NULL,
        size INTEGER NOT NULL,
        present BOOLEAN NOT NULL,
        PRIMARY KEY(file_id, offset)
    );
    CREATE INDEX idx_blocks_file_id ON blocks(file_id);
    CREATE INDEX idx_blocks_hash ON blocks(hash);
    CREATE INDEX idx_blocks_offset ON blocks(file_id, offset);
    CREATE INDEX idx_blocks_present ON blocks(file_id, present);

    PRAGMA application_id=0x51367457;
    PRAGMA user_version=0x00000000;
";

pub const ZPAQ_BITS: usize = 13; // 13 bits = 8 KiB block average
pub const MAX_BLOCK_SIZE: usize = 1 << 15; // 32 KiB

/// Index of files and blocks
pub struct Index {
    db: Connection,
    in_transaction: bool,
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
        Ok(Index { db, in_transaction: false })
    }

    /// Open an in-memory index
    pub fn open_in_memory() -> Result<Index, Error> {
        let db = Connection::open_in_memory()?;
        db.execute_batch(SCHEMA)?;
        Ok(Index { db, in_transaction: false })
    }

    pub fn begin(&mut self) -> Result<(), Error> {
        if !self.in_transaction {
            self.db.execute_batch("BEGIN IMMEDIATE;")?;
            self.in_transaction = true;
        }
        Ok(())
    }

    /// Try to find a block in the indexed files
    pub fn get_block(
        &self,
        hash: &HashDigest,
    ) -> Result<Option<(PathBuf, usize, usize)>, Error> {
        let mut stmt = self.db.prepare(
            "
            SELECT files.name, blocks.offset, blocks.size
            FROM blocks
            INNER JOIN files ON blocks.file_id = files.file_id
            WHERE blocks.hash = ? AND blocks.present = 1;
            ",
        )?;
        let mut rows = stmt.query(&[hash as &dyn ToSql])?;
        if let Some(row) = rows.next() {
            let row = row?;
            let path: String = row.get(0);
            let path: PathBuf = path.into();
            let offset: i64 = row.get(1);
            let offset = offset as usize;
            let size: i64 = row.get(2);
            let size = size as usize;
            Ok(Some((path, offset, size)))
        } else {
            Ok(None)
        }
    }

    /// Try to get a file from its name
    pub fn get_file(
        &self,
        name: &Path,
    ) -> Result<Option<(u32, chrono::DateTime<chrono::Utc>, HashDigest)>, Error> {
        let mut stmt = self.db.prepare(
            "
            SELECT file_id, modified, blocks_hash FROM files
            WHERE name = ?;
            ",
        )?;
        let mut rows = stmt.query(&[name.to_str().expect("encoding")])?;
        if let Some(row) = rows.next() {
            let row = row?;
            let file_id = row.get(0);
            let modified = row.get(1);
            let blocks_hash = row.get(2);
            Ok(Some((file_id, modified, blocks_hash)))
        } else {
            Ok(None)
        }
    }

    /// Add a file to the index
    ///
    /// This returns a tuple `(file_id, up_to_date)` where `file_id` can be
    /// used to insert blocks, and `up_to_date` indicates whether the file's
    /// modification date has changed and it should be re-indexed.
    pub fn add_file(
        &mut self,
        name: &Path,
        modified: chrono::DateTime<chrono::Utc>,
    ) -> Result<(u32, bool), Error> {
        self.begin()?;
        if let Some((file_id, old_modified, _)) = self.get_file(name)? {
            if old_modified != modified {
                info!("Resetting file {:?}, modified", name);
                // Delete blocks
                self.db.execute(
                    "
                    DELETE FROM blocks WHERE file_id = ?;
                    ",
                    &[&file_id],
                )?;
                // Update modification time
                self.db.execute(
                    "
                    UPDATE files
                    SET modified = ?, size = NULL, blocks_hash = NULL
                    WHERE file_id = ?;
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
            self.db.execute(
                "
                INSERT INTO files(name, modified)
                VALUES(?, ?);
                ",
                &[&name.to_str().expect("encoding") as &dyn ToSql, &modified],
            )?;
            let file_id = self.db.last_insert_rowid();
            Ok((file_id as u32, false))
        }
    }

    /// Replace file in the index
    ///
    /// This is like add_file but will always replace an existing file.
    pub fn add_file_overwrite(
        &mut self,
        name: &Path,
        modified: chrono::DateTime<chrono::Utc>,
    ) -> Result<u32, Error> {
        self.begin()?;
        if let Some((file_id, _, _)) = self.get_file(name)? {
            info!("Resetting file {:?}", name);
            // Delete blocks
            self.db.execute(
                "
                DELETE FROM blocks WHERE file_id = ?;
                ",
                &[&file_id],
            )?;
            // Update modification time
            self.db.execute(
                "
                UPDATE files
                SET modified = ?, size = NULL, blocks_hash = NULL
                WHERE file_id = ?;
                ",
                &[&modified as &dyn ToSql, &file_id],
            )?;
            Ok(file_id)
        } else {
            info!("Inserting new file {:?}", name);
            self.db.execute(
                "
                INSERT INTO files(name, modified)
                VALUES(?, ?);
                ",
                &[&name.to_str().expect("encoding") as &dyn ToSql, &modified],
            )?;
            let file_id = self.db.last_insert_rowid();
            Ok(file_id as u32)
        }
    }

    /// Remove a file and all its blocks from the index
    pub fn remove_file(&mut self, file_id: u32) -> Result<(), Error> {
        self.begin()?;
        self.db.execute(
            "
            DELETE FROM blocks WHERE file_id = ?;
            ",
            &[&file_id],
        )?;
        self.db.execute(
            "
            DELETE FROM files WHERE file_id = ?;
            ",
            &[&file_id],
        )?;
        Ok(())
    }

    /// Move a file, possibly over another
    pub fn move_file(
        &mut self,
        file_id: u32,
        destination: &Path,
    ) -> Result<(), Error> {
        self.begin()?;
        let destination = destination.to_str().expect("encoding");

        // Delete old file
        self.db.execute(
            "
            DELETE FROM blocks WHERE file_id = (
                SELECT file_id FROM files
                WHERE name = ?
            );
            ",
            &[destination],
        )?;
        self.db.execute(
            "
            DELETE FROM files WHERE name = ?;
            ",
            &[destination],
        )?;

        // Move new file
        self.db.execute(
            "
            UPDATE files SET name = ?
            WHERE file_id = ?;
            ",
            &[&destination as &dyn ToSql, &file_id],
        )?;
        Ok(())
    }

    /// Get a list of all the files in the index
    pub fn list_files(
        &self,
    ) -> Result<Vec<(u32, PathBuf, chrono::DateTime<chrono::Utc>, usize, HashDigest)>, Error>
    {
        let mut stmt = self.db.prepare(
            "
            SELECT file_id, name, modified, size, blocks_hash FROM files;
            ",
        )?;
        let mut rows = stmt.query(rusqlite::NO_PARAMS)?;
        let mut results = Vec::new();
        loop {
            match rows.next() {
                Some(Ok(row)) => {
                    let path: String = row.get(1);
                    let size: Option<i64> = row.get(3);
                    results.push((row.get(0), path.into(), row.get(2), size.unwrap_or(0) as usize, row.get(4)))
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
        hash: &HashDigest,
        file_id: u32,
        offset: usize,
        size: usize,
    ) -> Result<(), Error> {
        self.begin()?;
        self.db.execute(
            "
            INSERT INTO blocks(hash, file_id, offset, size, present)
            VALUES(?, ?, ?, ?, 1);
            ",
            &[
                &hash as &dyn ToSql,
                &file_id,
                &(offset as i64),
                &(size as i64),
            ],
        )?;
        Ok(())
    }

    /// Get a list of all the blocks in a specific file
    pub fn list_file_blocks(
        &self,
        file_id: u32,
    ) -> Result<Vec<(HashDigest, usize, usize)>, Error> {
        let mut stmt = self.db.prepare(
            "
            SELECT hash, offset, size FROM blocks
            WHERE file_id = ?;
            ",
        )?;
        let mut rows = stmt.query(&[file_id])?;
        let mut results = Vec::new();
        loop {
            match rows.next() {
                Some(Ok(row)) => {
                    let offset: i64 = row.get(1);
                    let size: i64 = row.get(2);
                    results.push((row.get(0), offset as usize, size as usize))
                }
                Some(Err(e)) => return Err(e.into()),
                None => break,
            }
        }
        Ok(results)
    }

    /// Get a list of files for which we don't have contents
    pub fn list_temp_files(&self) -> Result<Vec<PathBuf>, Error> {
        let mut stmt = self.db.prepare(
            "
            SELECT name FROM files
            WHERE substr(name, 1, 14) = '.syncfast_tmp_';
            ",
        )?;
        let mut rows = stmt.query(rusqlite::NO_PARAMS)?;
        let mut results = Vec::new();
        loop {
            match rows.next() {
                Some(Ok(row)) => {
                    let name: String = row.get(0);
                    results.push(name.into());
                }
                Some(Err(e)) => return Err(e.into()),
                None => break,
            }
        }
        Ok(results)
    }

    /// Get a list of blocks that are referenced by files but not present
    pub fn list_missing_blocks(&self) -> Result<Vec<HashDigest>, Error> {
        let mut stmt = self.db.prepare(
            "
            SELECT hash FROM blocks
            WHERE present = 0;
            ",
        )?;
        let mut rows = stmt.query(rusqlite::NO_PARAMS)?;
        let mut results = Vec::new();
        loop {
            match rows.next() {
                Some(Ok(row)) => {
                    let hash: HashDigest = row.get(0);
                    results.push(hash);
                },
                Some(Err(e)) => return Err(e.into()),
                None => break,
            }
        }
        Ok(results)
    }

    /// Get all locations where a block is to be found
    pub fn list_block_locations(
        &self,
        hash: &HashDigest,
    ) -> Result<Vec<(PathBuf, usize, usize)>, Error> {
        let mut stmt = self.db.prepare(
            "
            SELECT files.name, offset, size
            FROM blocks
            INNER JOIN files ON files.file_id = blocks.file_id
            WHERE hash = ?;
            ",
        )?;
        let mut rows = stmt.query(&[hash])?;
        let mut results = Vec::new();
        loop {
            match rows.next() {
                Some(Ok(row)) => {
                    let name: String = row.get(0);
                    let offset: i64 = row.get(1);
                    let size: i64 = row.get(2);
                    results.push((name.into(), offset as usize, size as usize));
                }
                Some(Err(e)) => return Err(e.into()),
                None => break,
            }
        }
        Ok(results)
    }

    /// Cut up a file into blocks and add them to the index
    pub fn index_file(
        &mut self,
        path: &Path,
        name: &Path,
    ) -> Result<(), Error> {
        let file = File::open(path)?;
        let (file_id, up_to_date) = self.add_file(
            name,
            file.metadata()?.modified()?.into(),
        )?;
        if !up_to_date {
            // Use ZPAQ to cut the stream into blocks
            let chunker = Chunker::new(
                ZPAQ::new(ZPAQ_BITS), // 13 bits = 8 KiB block average
            ).max_size(MAX_BLOCK_SIZE);
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
                        let size = offset - start_offset;
                        debug!(
                            "Adding block, offset={}, size={}, sha1={}",
                            start_offset, size, digest,
                        );
                        self.add_block(&digest, file_id, start_offset, size)?;
                        start_offset = offset;
                        sha1.reset();
                    }
                }
            }

            // Compute blocks_hash
            sha1.reset();
            let mut stmt = self.db.prepare(
                "
                SELECT hash FROM blocks WHERE file_id = ?;
                ",
            )?;
            let mut rows = stmt.query(&[file_id])?;
            loop {
                match rows.next() {
                    Some(Ok(row)) => {
                        let digest: HashDigest = row.get(0);
                        sha1.update(&digest.0);
                    }
                    Some(Err(e)) => return Err(e.into()),
                    None => break,
                }
            }
            let blocks_digest = HashDigest(sha1.digest().bytes());
            self.db.execute(
                "
                UPDATE files SET blocks_hash = ? WHERE file_id = ?;
                ",
                &[&blocks_digest as &dyn ToSql, &file_id],
            )?;
        }
        Ok(())
    }

    /// Index files and directories recursively
    pub fn index_path(&mut self, path: &Path) -> Result<(), Error> {
        self.index_path_rec(path, Path::new(""))
    }

    fn index_path_rec(
        &mut self,
        root: &Path,
        rel: &Path,
    ) -> Result<(), Error> {
        let path = root.join(rel);
        if path.is_dir() {
            info!("Indexing directory {:?} ({:?})", rel, path);
            for entry in path.read_dir()? {
                if let Ok(entry) = entry {
                    if entry.file_name() == ".syncfast.idx" {
                        continue;
                    }
                    self.index_path_rec(root, &rel.join(entry.file_name()))?;
                }
            }
            Ok(())
        } else {
            let rel = if rel.starts_with(".") {
                rel.strip_prefix(".").unwrap()
            } else {
                rel
            };
            info!("Indexing file {:?} ({:?})", rel, path);
            self.index_file(&path, &rel)
        }
    }

    /// List all files and remove those that don't exist on disk
    pub fn remove_missing_files(&mut self, path: &Path) -> Result<(), Error> {
        for (file_id, file_path, _modified, _size, _blocks_hash) in self.list_files()? {
            if !path.join(&file_path).is_file() {
                info!("Removing missing file {:?}", file_path);
                self.remove_file(file_id)?;
            }
        }
        Ok(())
    }

    /// Commit the transaction
    pub fn commit(&mut self) -> Result<(), rusqlite::Error> {
        if self.in_transaction {
            self.db.execute_batch("COMMIT")?;
            self.in_transaction = false;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::path::Path;
    use tempfile::NamedTempFile;

    use crate::HashDigest;
    use super::{Index, MAX_BLOCK_SIZE};

    #[test]
    fn test() {
        let mut file = NamedTempFile::new().expect("tempfile");
        for i in 0 .. 2000 {
            writeln!(file, "Line {}", i + 1).expect("tempfile");
        }
        for _ in 0 .. 2000 {
            writeln!(file, "Test content").expect("tempfile");
        }
        file.flush().expect("tempfile");
        let name = Path::new("dir/name").to_path_buf();
        let mut index = Index::open_in_memory().expect("db");
        index.index_file(file.path(), &name).expect("index");
        index.commit().expect("db");
        assert!(index
            .get_block(&HashDigest(*b"12345678901234567890"))
            .expect("get")
            .is_none());
        let block1 = index
            .get_block(&HashDigest(
                *b"\xfb\x5e\xf7\xeb\xad\xd8\x2c\x80\x85\xc5\
               \xff\x63\x82\x36\x22\xba\xe0\xe2\x63\xf6",
            ))
            .expect("get");
        assert_eq!(block1, Some((name.clone(), 0, 11579)),);
        let block2 = index
            .get_block(&HashDigest(
                *b"\x57\x0d\x8b\x30\xfc\xfd\x58\x5e\x41\x27\
               \xb5\x61\xf5\xec\xd3\x76\xff\x4d\x01\x01",
            ))
            .expect("get");
        assert_eq!(block2, Some((name.clone(), 11579, 32768)),);
        let block3 = index
            .get_block(&HashDigest(
                *b"\xb9\xa8\xc2\x64\x1a\xf2\xcf\x8f\xd8\xf3\
               \x6a\x24\x56\xa3\xea\xa9\x5c\x02\x91\x27",
            ))
            .expect("get");
        assert_eq!(block3, Some((name.clone(), 44347, 546)),);
        assert_eq!(block3.unwrap().1 - block2.unwrap().1, MAX_BLOCK_SIZE);
        let file1 = index.get_file(&name).expect("db").expect("get_file");
        assert_eq!(file1.0, 1);
        assert_eq!(file1.2, HashDigest(
            *b"\x84\xC2\x5D\x78\xED\xCD\xB6\x76\x31\x63\
            \x9C\x43\x60\x4C\xF0\x14\x95\x64\xF0\x44",
        ));
    }
}
