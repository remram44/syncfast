use std::collections::{HashMap, HashSet};
use std::io::{self, Read, Seek, Write};
use std::path::Path;

use adler32::RollingAdler32;
use byteorder::{WriteBytesExt, BigEndian};
use log::LogLevel;
use utils::{copy, CopyMode, ReadExt, to_hex};
use sha1::Sha1;

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

    let mut back_blocks: HashMap<u32, HashMap<[u8; 20], u64>> = HashMap::new();

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

        // SHA-1 function: gets SHA-1 digest for current block
        // Only computed if we found an Adler32 match
        let get_sha1 = |pos: u64, block_start: u64, buffer: &[u8]| -> [u8; 20] {
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

        // Now we advance while updating the Adler32 hash, until we find a
        // known block or we read 2**16 bytes
        loop {
            enum Match {
                No,
                Old,
                New(u64)
            }
            let mut sha1 = None;
            let mut match_what = Match::No;
            if let Some(sha1_hashes) = back_blocks.get(&adler32.hash()) {
                info!("Found backref Adler32 at position {}-{}: {}",
                      pos - read as u64, pos, adler32.hash());
                sha1 = Some(get_sha1(pos, block_start, &buffer));
                if let Some(offset) = sha1_hashes.get(sha1.as_ref().unwrap()) {
                    info!("SHA-1 matches; old position: {}", offset);
                    match_what = Match::New(offset.clone());
                } else {
                    let hashes = sha1_hashes.iter().fold(
                        String::new(),
                        |mut s, (i, _)| {
                            s.push(' ');
                            s.push_str(&to_hex(i));
                            s
                        });
                    info!("SHA-1 doesn't match: found {} != {}",
                          to_hex(sha1.as_ref().unwrap()), hashes);
                }
            }
            if let Match::No = match_what {
                if let Some(sha1_hashes) = hashes.get(&adler32.hash()) {
                    info!("Found known Adler32 match at position {}-{}: {}",
                          pos - read as u64, pos, adler32.hash());
                    if sha1.is_none() {
                        sha1 = Some(get_sha1(pos, block_start, &buffer));
                    }
                    if sha1_hashes.contains(sha1.as_ref().unwrap()) {
                        info!("SHA-1 matches");
                        match_what = Match::Old;
                    }
                }
            }

            match match_what {
                Match::No => {}
                _ => {
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
                }
            }

            match match_what {
                Match::No => {
                    if (pos - block_start) as usize >= 65536 {
                        // Write the whole block, so as to not overflow the u16
                        // block length field
                        let len = 65536;
                        info!("No match at position {}, writing unmatched \
                               block, size {}", pos, len);
                        try!(delta.write_u8(0x01)); // LITERAL
                        try!(delta.write_u16::<BigEndian>(0xFFFF));
                        try!(file.seek(io::SeekFrom::Start(block_start)));
                        try!(copy(file, delta, CopyMode::Exact(len)));
                        try!(file.seek(io::SeekFrom::Start(pos)));
                        break;
                    }
                }
                Match::Old => {
                    // Write the reference to the known block
                    let sha1 = sha1.as_ref().unwrap();
                    if log_enabled!(LogLevel::Info) {
                        info!("Writing known block, Adler32: {}, SHA-1: {}",
                              adler32.hash(), to_hex(sha1));
                    }
                    try!(delta.write_u8(0x02)); // KNOWN_BLOCK
                    try!(delta.write_u32::<BigEndian>(adler32.hash()));
                    try!(delta.write_all(sha1));
                    break;
                }
                Match::New(offset) => {
                    if log_enabled!(LogLevel::Info) {
                        info!("Writing backref, offset: {}", offset);
                    }
                    try!(delta.write_u8(0x03)); // BACKREF
                    try!(delta.write_u64::<BigEndian>(offset));
                    break;
                }
            }

            if read == blocksize &&
                (pos - block_start) as usize % blocksize == 0
            {
                let adler32 = adler32.hash();
                let sha1 = get_sha1(pos, block_start, &buffer);
                let offset = pos - read as u64;
                info!("Recording back-ref to pos {}; Adler32: {}, SHA-1: {}",
                      offset, adler32, to_hex(&sha1));
                if match back_blocks.get_mut(&adler32) {
                    Some(hm) => {
                        info!("(Adler32 hashes collide)");
                        hm.insert(sha1, offset);
                        false
                    }
                    None => true,
                } {
                    let mut hm = HashMap::new();
                    hm.insert(sha1, offset);
                    assert!(back_blocks.insert(adler32, hm).is_none());
                }
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
