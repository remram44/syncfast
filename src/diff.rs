use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{self, Read, Seek, Write};
use std::iter::once;
use std::path::Path;

use adler32::RollingAdler32;
use byteorder::{self, ReadBytesExt, WriteBytesExt, BigEndian};
use log::LogLevel;
use super::{Adler32_SHA1, adler32_sha1, DefaultHashes};
use utils::{copy, CopyMode, ReadExt, to_hex};
use sha1::Sha1;

/// Hashes files into a Hashes structure from an iterator of filenames.
pub fn hash_files<P: AsRef<Path>, I: Iterator<Item=P>>(filenames: I,
                                                       blocksize: usize)
    -> io::Result<DefaultHashes>
{
    info!("Creating index, blocksize = {}", blocksize);
    let mut hashes: DefaultHashes = DefaultHashes::new(adler32_sha1,
                                                       blocksize);
    for filename in filenames {
        let path = filename.as_ref().to_owned();
        info!("Indexing {}", path.to_string_lossy());
        let f = try!(File::open(&path));
        try!(hashes.hash(path, f));
    }
    Ok(hashes)
}

/// Serializes a Hashes structure into an index file.
pub fn write_index_file(index: File, hashes: DefaultHashes) -> io::Result<()> {
    info!("Writing index file: {} hashes", hashes.blocks().len());
    let mut index = io::BufWriter::new(index);
    try!(index.write_all(b"RS-SYNCI"));
    try!(index.write_u16::<BigEndian>(0x0001)); // 0.1
    try!(index.write_u32::<BigEndian>(hashes.blocksize() as u32));
    try!(index.write_u32::<BigEndian>(hashes.blocks().len() as u32));
    for h in hashes.blocks().keys() {
        try!(index.write_u32::<BigEndian>(h.adler32));
        try!(index.write_all(&h.sha1));
    }
    Ok(())
}

/// Read an index file into an object for Adler32 then SHA-1 lookups.
pub fn read_index_file<R: Read>(index: R)
    -> io::Result<(HashMap<u32, HashSet<[u8; 20]>>, usize)>
{
    let mut hashes: HashMap<u32, HashSet<[u8; 20]>> = HashMap::new();
    let mut index = io::BufReader::new(index);
    let mut buffer = [0u8; 8];
    try!(index.read_exact_(&mut buffer));
    if &buffer != b"RS-SYNCI" {
        return Err(io::Error::new(io::ErrorKind::InvalidData,
                                  "Invalid index file"));
    }
    let version = try!(index.read_u16::<BigEndian>());
    if version != 0x0001 { // 0.1
        return Err(io::Error::new(io::ErrorKind::InvalidData,
                                  format!("Index file in unknown version \
                                           {}.{}",
                                          version >> 8, version & 0xFF)));
    }
    let blocksize = try!(index.read_u32::<BigEndian>()) as usize;
    let nb_hashes = try!(index.read_u32::<BigEndian>());
    info!("Index file is version {}.{}. blocksize = {}, {} hashes",
          version >> 8, version & 0xFF, blocksize, nb_hashes);
    for _ in 0..nb_hashes {
        let adler32 = try!(index.read_u32::<BigEndian>());
        info!("Read Adler32: {}", adler32);
        let mut sha1 = [0u8; 20];
        if try!(index.read(&mut sha1)) != 20 {
            return Err(io::Error::new(io::ErrorKind::InvalidData,
                                      "Unexpected end of file"));
        }
        if log_enabled!(LogLevel::Info) {
            info!("Read SHA-1: {}", to_hex(&sha1));
        }

        if match hashes.get_mut(&adler32) {
            Some(set) => {
                info!("(Adler32 hashes collide)");
                set.insert(sha1);
                false
            }
            None => true,
        } {
            let mut set = HashSet::new();
            set.insert(sha1);
            assert!(hashes.insert(adler32, set).is_none());
        }
    }
    try!(index.read_eof());
    Ok((hashes, blocksize))
}

/// Write a delta file in "single-file mode" from an index and a single input.
pub fn write_delta_file_single<I: Read + Seek, O: Write>(
        hashes: &HashMap<u32, HashSet<[u8; 20]>>, mut file: I,
        delta: &mut O, blocksize: usize)
    -> io::Result<()>
{
    try!(delta.write_all(b"RS-SYNCD"));
    try!(delta.write_u16::<BigEndian>(0x0001)); // 0.1
    try!(delta.write_u32::<BigEndian>(blocksize as u32));
    try!(delta.write_u16::<BigEndian>(0)); // Single-file mode

    write_delta(&hashes, &mut file, delta, blocksize)
}

/// Write a delta file in "directory mode" from an index and a list of paths.
pub fn write_delta_file_multiple<'a, P, I, O: Write>(
        hashes: &HashMap<u32, HashSet<[u8; 20]>>, files: I,
        delta: &mut O, blocksize: usize)
    -> io::Result<()>
    where P: AsRef<Path>, I: Iterator<Item=P>
{
    try!(delta.write_all(b"RS-SYNCD"));
    try!(delta.write_u16::<BigEndian>(0x0001)); // 0.1
    try!(delta.write_u32::<BigEndian>(blocksize as u32));
    try!(delta.write_u16::<BigEndian>(0)); // Single-file mode

    unimplemented!();
}

/// Writes a single file entry to the delta file, from the index and file.
fn write_delta<I: Read + Seek, O: Write>(
        hashes: &HashMap<u32, HashSet<[u8; 20]>>, file: &mut I, delta: &mut O,
        blocksize: usize)
    -> io::Result<()>
{
    let mut pos: u64 = 0;

    // Reads the file by blocks
    loop {
        let block_start = pos;
        info!("Now at position {}", pos);

        // Reads max one block
        let mut buffer = vec![0u8; blocksize];
        let read = try!(file.read_retry(&mut buffer));
        if read == 0 {
            info!("End of file");
            try!(delta.write_u8(0x00)); // ENDFILE
            return Ok(());
        }
        info!("Starting scan");
        pos += read as u64;

        // Hash it
        let mut adler32 = RollingAdler32::from_buffer(&buffer[..read]);

        // Now we advance while updating the Adler32 hash, until we find a
        // known block or we read 2**16 bytes
        loop {
            if let Some(sha1_hashes) = hashes.get(&adler32.hash()) {
                info!("Found Adler32 match at position {}: {}",
                      pos, adler32.hash());
                let sha1 = {
                    let buf_pos = ((pos - block_start) as usize
                                   - read as usize) % blocksize;
                    let mut hasher = Sha1::new();
                    if read == blocksize {
                        hasher.update(&buffer[buf_pos..]);
                        hasher.update(&buffer[..buf_pos]);
                    } else {
                        assert!(buf_pos == 0);
                        hasher.update(&buffer[..read]);
                    }
                    let mut digest = [0u8; 20];
                    hasher.output(&mut digest);
                    digest
                };

                if sha1_hashes.contains(&sha1) {
                    info!("SHA-1 matches");

                    // Write the unmatched part up to the known block
                    if (pos - block_start) as usize > read {
                        let len = (pos - block_start) as usize - read;
                        info!("Writing unmatched block, size {}", len);
                        try!(delta.write_u8(0x01)); // LITERAL
                        try!(delta.write_u16::<BigEndian>((len - 1) as u16));
                        try!(file.seek(io::SeekFrom::Start(block_start)));
                        try!(copy(file, delta,
                                  CopyMode::Exact(len)));
                        try!(file.seek(io::SeekFrom::Start(pos)));
                    }

                    // Write the reference to the known block
                    if log_enabled!(LogLevel::Info) {
                        info!("Writing known block, Adler32: {}, SHA-1: {}",
                              adler32.hash(), to_hex(&sha1));
                    }
                    try!(delta.write_u8(0x02)); // KNOWN_BLOCK
                    try!(delta.write_u32::<BigEndian>(adler32.hash()));
                    try!(delta.write_all(&sha1));
                    break;
                } else {
                    let hashes = sha1_hashes.iter().fold(
                        String::new(),
                        |mut s, i| { s.push(' '); s.push_str(&to_hex(i)); s });
                    info!("SHA-1 doesn't match: found {} !={}",
                          to_hex(&sha1), hashes);
                }
            } else if (pos - block_start) as usize >= 65536 {
                // Write the whole block, so as to not overflow the u16 block
                // length field
                let len = 65536;
                info!("No match at position {}, writing unmatched block, \
                       size {}", pos, len);
                try!(delta.write_u8(0x01)); // LITERAL
                try!(delta.write_u16::<BigEndian>(0xFFFF));
                try!(file.seek(io::SeekFrom::Start(block_start)));
                try!(copy(file, delta, CopyMode::Exact(len)));
                try!(file.seek(io::SeekFrom::Start(pos)));
                break;
            }

            {
                let idx = (pos % (blocksize as u64)) as usize;
                adler32.remove(blocksize, buffer[idx]);
                if try!(file.read(&mut buffer[idx..idx + 1])) == 0 {
                    // End of file, write last unknown block
                    let len = (pos - block_start) as usize;
                    if len > 0 {
                        info!("Writing last block from position {}, size {}",
                              block_start, len);
                        try!(delta.write_u8(0x01)); // LITERAL
                        try!(delta.write_u16::<BigEndian>((len - 1) as u16));
                        try!(file.seek(io::SeekFrom::Start(block_start)));
                        try!(copy(file, delta, CopyMode::Exact(len)));
                        try!(file.seek(io::SeekFrom::Start(pos)));
                    }
                    break;
                }
                adler32.update(buffer[idx]);
            }
            pos += 1;
        }
    }
}

/// Apply the delta to a file to get the new file.
pub fn apply_diff<'a, I: Iterator<Item=&'a Path>>(
        references: I, old_file: &'a Path,
        delta_file: &'a Path, new_file: &'a Path)
    -> io::Result<()>
{
    // Read the delta file
    let mut delta = io::BufReader::new(try!(File::open(delta_file)));
    let mut buffer = [0u8; 8];
    try!(delta.read_exact_(&mut buffer));
    if &buffer != b"RS-SYNCD" {
        return Err(io::Error::new(io::ErrorKind::InvalidData,
                                  "Invalid delta file"));
    }
    let version = try!(delta.read_u16::<BigEndian>());
    if version != 0x0001 { // 0.1
        return Err(io::Error::new(io::ErrorKind::InvalidData,
                                  format!("Delta file is in unknown version \
                                           {}.{}",
                                          version >> 8, version & 0xFF)));
    }
    let blocksize = try!(delta.read_u32::<BigEndian>()) as usize;
    if try!(delta.read_u16::<BigEndian>()) != 0 {
        return Err(io::Error::new(io::ErrorKind::InvalidData,
                                  "Delta file has multiple files, which is \
                                   not yet supported"));
    }

    // Hash all the reference files
    let hashes = try!(hash_files(once(old_file).chain(references),
                                 blocksize));

    // Open the new file
    let mut file = try!(File::create(new_file));

    loop {
        match try!(delta.read_u8()) {
            0x00 => break,
            0x01 => { // LITERAL
                info!("Literal block");
                let len = try!(delta.read_u16::<BigEndian>()) as usize + 1;
                info!("Size: {}", len);
                try!(copy(&mut delta, &mut file, CopyMode::Exact(len)));
            }
            0x02 => { // KNOWN_BLOCK
                info!("Known block");
                let adler32 = match delta.read_u32::<BigEndian>() {
                    Err(byteorder::Error::UnexpectedEOF) => {
                        return Err(io::Error::new(io::ErrorKind::InvalidData,
                                                  "Unexpected end of file"));
                    }
                    Err(byteorder::Error::Io(e)) => return Err(e),
                    Ok(n) => n,
                };
                let sha1 = {
                    let mut buf = [0u8; 20];
                    if try!(delta.read_retry(&mut buf)) != 20 {
                        return Err(io::Error::new(io::ErrorKind::InvalidData,
                                                  "Unexpected end of file"));
                    }
                    buf
                };
                info!("Adler32: {}, SHA-1: {}", adler32, to_hex(&sha1));
                match hashes.find(&Adler32_SHA1 { adler32: adler32,
                                                  sha1: sha1 }) {
                    None => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "Delta file references unknown block hash; did \
                             you forget --reference arguments? Did any of the \
                             source files change?"));
                    }
                    Some(loc) => {
                        let mut origin = try!(File::open(&loc.file));
                        try!(origin.seek(io::SeekFrom::Start(loc.offset)));
                        let copied = try!(copy(&mut origin, &mut file,
                                               CopyMode::Maximum(blocksize)));
                        info!("Copied {} bytes", copied);
                    }
                }
            }
            c => {
                error!("Invalid command {:02X}", c);
                return Err(io::Error::new(io::ErrorKind::InvalidData,
                                          "Invalid delta command"));
            }
        }
    }
    try!(delta.read_eof());
    Ok(())
}
