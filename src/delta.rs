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
