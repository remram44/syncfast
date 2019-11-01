//! Synchronization from and to local files.

use cdchunking::{Chunker, ZPAQ};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::collections::hash_map::{Entry, HashMap};
use std::fs::{DirBuilder, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::ops::Not;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use crate::{Error, HashDigest};
use crate::index::{MAX_BLOCK_SIZE, ZPAQ_BITS, Index, IndexTransaction};
use crate::sync::{IndexEvent, Sink, SinkWrapper, Source, SourceWrapper};

struct TempFile {
    file: File,
    temp_file_id: u32,
    temp_path: PathBuf,
    name: PathBuf,
}

impl TempFile {
    fn move_to_destination(
        self,
        root_dir: &Path,
    ) -> Result<(), std::io::Error> {
        std::fs::rename(self.temp_path, root_dir.join(self.name))
    }
}

fn read_block(path: &Path, offset: usize) -> Result<Vec<u8>, Error> {
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(offset as u64))?;
    let chunker = Chunker::new(
        ZPAQ::new(ZPAQ_BITS),
    ).max_size(MAX_BLOCK_SIZE);
    let block = chunker.whole_chunks(file).next().unwrap()?;
    Ok(block)
}

fn write_block(
    file: &mut File,
    offset: usize,
    block: &[u8],
) -> Result<(), Error> {
    // FIXME: Can you seek past the end?
    file.seek(SeekFrom::Start(offset as u64))?;
    file.write_all(block)?;
    Ok(())
}

struct BlockDestination {
    temp_file: Rc<RefCell<TempFile>>,
    offset: usize,
}

/// Local filesystem sink, e.g. `Sink` that writes files.
pub struct FsSink<'a> {
    index: IndexTransaction<'a>,
    root_dir: &'a Path,
    current_file: Option<(usize, Rc<RefCell<TempFile>>)>,
    waiting_blocks: HashMap<HashDigest, Vec<BlockDestination>>,
    blocks_to_request: VecDeque<HashDigest>,
}

impl<'a> FsSink<'a> {
    /// Create a sink from the (destination) index
    pub fn new(index: IndexTransaction<'a>, root_dir: &'a Path) -> FsSink<'a> {
        FsSink {
            index,
            root_dir,
            current_file: None,
            waiting_blocks: HashMap::new(),
            blocks_to_request: VecDeque::new(),
        }
    }

    fn finish_file(&mut self, file: TempFile) -> Result<(), Error> {
        info!("File complete: {:?}", file.name);
        self.index.move_file(file.temp_file_id, &file.name)?;
        file.move_to_destination(self.root_dir)?;
        Ok(())
    }

    fn end_current_file(&mut self) -> Result<(), Error> {
        if let Some((size, file)) = self.current_file.take() {
            debug!("File {:?} will be {} bytes", file.borrow().name, size);

            // If we have the last reference to that TempFile, move it
            if let Ok(file) = Rc::try_unwrap(file) {
                self.finish_file(file.into_inner())?;
            }
        }
        Ok(())
    }
}

impl<'a> Sink for FsSink<'a> {
    fn new_file(
        &mut self,
        name: &Path,
        modified: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), Error> {
        info!("Next file: {:?}", name);

        self.end_current_file()?;

        // Make temp file path, which will be swapped at the end
        let temp_name = {
            let mut base_name = name
                .file_name()
                .ok_or_else(|| {
                    Error::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Invalid file name",
                    ))
                })?
                .to_os_string();
            base_name.push(".part");
            name.with_file_name(base_name)
        };
        let temp_path = self.root_dir.join(&temp_name);

        // Open it, but check if it existed
        let file_exists = temp_path.is_file();
        if let Some(dir) = temp_path.parent() {
            DirBuilder::new().recursive(true).create(dir)?;
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&temp_path)?;

        // Create temp file entry in the database
        let file_id = self.index.add_file_overwrite(&temp_name, modified)?;

        // If file existed, index; it might have content from aborted download
        if file_exists {
            info!("Indexing previous part file {:?}", temp_path);
            self.index.index_file(&temp_path, &temp_name)?;
        }

        let file = TempFile {
            file,
            temp_file_id: file_id,
            temp_path,
            name: name.to_path_buf(),
        };
        let file = Rc::new(RefCell::new(file));
        self.current_file = Some((0, file));
        Ok(())
    }

    fn new_block(
        &mut self,
        hash: &HashDigest,
        size: usize,
    ) -> Result<(), Error> {
        let &mut (ref mut offset, ref mut file) = &mut self.current_file
            .as_mut()
            .ok_or_else(|| {
                Error::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Got a block before any file",
                ))
            })?;

        info!(
            "Next block: {} for {:?} offset={}",
            hash, file.borrow().name, *offset,
        );

        // We need to write this block to the current file
        match self.index.get_block(hash)? {
            // We know where to get it, copy it from there
            Some((name, read_offset, _size)) => {
                info!("Getting block from {:?} offset={}", name, read_offset);
                let block = read_block(
                    &self.root_dir.join(name),
                    read_offset,
                )?;
                write_block(&mut file.borrow_mut().file, *offset, &block)?;
                self.index.replace_block(
                    &hash,
                    file.borrow().temp_file_id,
                    *offset,
                    block.len(),
                )?;
            }
            // We don't have this block, we'll have to wait for it
            None => {
                // Was it already requested?
                match self.waiting_blocks.entry(hash.clone()) {
                    Entry::Occupied(ref mut destinations) => {
                        // Add this to the list of where to write the block
                        destinations.get_mut().push(BlockDestination {
                            temp_file: file.clone(),
                            offset: *offset,
                        });
                        info!("Block has already been requested");
                    }
                    Entry::Vacant(v) => {
                        // Request it
                        v.insert(vec![BlockDestination {
                            temp_file: file.clone(),
                            offset: *offset,
                        }]);
                        self.blocks_to_request.push_back(hash.clone());
                        info!("Requesting block");
                    }
                }
            }
        }
        *offset += size;
        Ok(())
    }

    fn end_files(&mut self) -> Result<(), Error> {
        self.end_current_file()
    }

    fn feed_block(
        &mut self,
        hash: &HashDigest,
        block: &[u8],
    ) -> Result<(), Error> {
        // Write the block to the destinations waiting for it
        if let Some(destinations) = self.waiting_blocks.remove(hash) {
            info!("Got block {}", hash);
            for destination in destinations.into_iter() {
                let BlockDestination { temp_file: file, offset } = destination;
                info!(
                    "Writing block to {:?} offset={}",
                    file.borrow().name, offset,
                );
                write_block(&mut file.borrow_mut().file, offset, block)?;
                self.index.replace_block(
                    &hash,
                    file.borrow().temp_file_id,
                    offset,
                    block.len(),
                )?;

                // If we have the last reference to that TempFile, move it
                if let Ok(file) = Rc::try_unwrap(file) {
                    self.finish_file(file.into_inner())?;
                }
            }
        } else {
            warn!("Got block we didn't need: {}", hash);
        }
        Ok(())
    }

    fn next_requested_block(&mut self) -> Result<Option<HashDigest>, Error> {
        Ok(self.blocks_to_request.pop_front())
    }

    fn is_missing_blocks(&self) -> Result<bool, Error> {
        Ok(self.waiting_blocks.is_empty().not())
    }
}

/// Local filesystem source, e.g. `Source` that reads files
pub struct FsSource<'a> {
    index: IndexTransaction<'a>,
    root_dir: &'a Path,
    files: VecDeque<(u32, PathBuf, chrono::DateTime<chrono::Utc>)>,
    blocks: VecDeque<(HashDigest, usize)>,
    requested_blocks: VecDeque<HashDigest>,
}

impl<'a> FsSource<'a> {
    /// Create a source from the (source) index
    pub fn new(
        index: IndexTransaction<'a>,
        root_dir: &'a Path,
    ) -> Result<FsSource<'a>, Error> {
        let files: VecDeque<_> = index.list_files()?.into_iter().collect();
        info!("Source indexed, {} files", files.len());
        Ok(FsSource {
            index,
            root_dir,
            files,
            blocks: VecDeque::new(),
            requested_blocks: VecDeque::new(),
        })
    }
}

impl<'a> Source for FsSource<'a> {
    fn next_from_index(&mut self) -> Result<Option<IndexEvent>, Error> {
        // If there are blocks left in current file, return one
        if let Some((hash, size)) = self.blocks.pop_front() {
            return Ok(Some(IndexEvent::NewBlock(hash, size)));
        }

        // If there are more files left, read the next one in
        if let Some((file_id, name, modified)) = self.files.pop_front() {
            let blocks = self.index.list_file_blocks(file_id)?;
            self.blocks = blocks
                .into_iter()
                .map(|(hash, _offset, size)| (hash, size))
                .collect();
            return Ok(Some(IndexEvent::NewFile(name, modified)));
        }

        // No more files
        Ok(Some(IndexEvent::End))
    }

    fn request_block(&mut self, hash: &HashDigest) -> Result<(), Error> {
        self.requested_blocks.push_back(hash.clone());
        Ok(())
    }

    fn get_next_block(
        &mut self,
    ) -> Result<Option<(HashDigest, Vec<u8>)>, Error> {
        match self.requested_blocks.pop_front() {
            Some(hash) => {
                if let Some((name, offset, _size)) =
                    self.index.get_block(&hash)?
                {
                    Ok(Some((
                        hash,
                        read_block(&self.root_dir.join(name), offset)?,
                    )))
                } else {
                    Err(Error::Io(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "Unknown block requested",
                    )))
                }
            }
            None => Ok(None),
        }
    }

    fn end(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

pub struct FsSinkWrapper {
    index: Index,
    path: PathBuf,
}

impl FsSinkWrapper {
    pub fn new(path: &Path) -> Result<FsSinkWrapper, Error> {
        let mut index = Index::open(&path.join(".rrsync.idx"))?;
        {
            let mut tx = index.transaction()?;
            info!(
                "Indexing destination into {:?}...",
                path.join(".rrsync.idx")
            );
            tx.index_path(path)?;
            tx.remove_missing_files(path)?;
            tx.commit()?;
        }
        let path = path.to_path_buf();
        Ok(FsSinkWrapper { index, path })
    }
}

impl SinkWrapper for FsSinkWrapper {
    fn open<'a>(&'a mut self) -> Result<Box<dyn Sink + 'a>, Error> {
        Ok(Box::new(FsSink::new(self.index.transaction()?, &self.path)))
    }
}

pub struct FsSourceWrapper {
    index: Index,
    path: PathBuf,
}

impl FsSourceWrapper {
    pub fn new(path: &Path) -> Result<FsSourceWrapper, Error> {
        let mut index = Index::open(&path.join(".rrsync.idx"))?;
        {
            let mut tx = index.transaction()?;
            info!("Indexing source into {:?}...", path.join(".rrsync.idx"));
            tx.index_path(path)?;
            tx.remove_missing_files(path)?;
            tx.commit()?;
        }
        let path = path.to_path_buf();
        Ok(FsSourceWrapper { index, path })
    }
}

impl SourceWrapper for FsSourceWrapper {
    fn open<'a>(&'a mut self) -> Result<Box<dyn Source + 'a>, Error> {
        Ok(Box::new(FsSource::new(
            self.index.transaction()?,
            &self.path,
        )?))
    }
}
