extern crate adler32;
extern crate byteorder;
extern crate docopt;
extern crate env_logger;
#[macro_use] extern crate log;
extern crate rs_sync;
extern crate rustc_serialize;
extern crate sha1;

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{self, Read, Seek, Write};
use std::path::Path;
use std::process;

use adler32::RollingAdler32;
use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};
use docopt::Docopt;
use log::LogLevel;
use rs_sync::{Adler32_SHA1, adler32_sha1, DefaultHashes};
use rs_sync::utils::{copy, CopyMode, ReadExt, to_hex};
use sha1::Sha1;

static USAGE: &'static str = "
rdiff clone.

Usage:
  rs-diff index [--blocksize=<b>] [--ref=<ref_file>]... <old-file> <index-file>
  rs-diff delta <index-file> <new-file> <delta-file>
  rs-diff patch [--ref=<ref>] <old-file> <delta-file> <new-file>
  rs-diff (-h | --help)
  rs-diff --version 

Options:
  -h --help             Show this screen.
  --blocksize=<bytes>   Blocksize in bytes [default: 4096]
";

#[derive(RustcDecodable)]
struct Args {
    cmd_index: bool,
    cmd_delta: bool,
    cmd_patch: bool,
    flag_ref: Vec<String>,
    flag_blocksize: usize,
    arg_old_file: String,
    arg_index_file: String,
    arg_new_file: String,
    arg_delta_file: String,
}

/// rs-diff program: offline delta computation and application.
///
/// This works similarly to the rdiff program.
///
/// First, the old files are hashed to compute an index file, which is just the
/// list of hashes of blocks that the receiver has.
///
/// Then, this index is compared to the new file to compute the delta file,
/// which contains a list of blocks of variable length that are either included
/// verbatim in the delta file, present in the old file (and thus appear in the
/// index), or appear previously in the new file (backreference).
///
/// Finally, the delta file is applied to the old file to get the new file, by
/// simply writing blocks from either the delta file, the old file or a
/// previous occurrence in the new file.
fn main() {
    env_logger::init().unwrap();

    let args: Args = Docopt::new(USAGE)
                            .and_then(|d| d.decode())
                            .unwrap_or_else(|e| {
                                 e.exit()
                             });

    let result = if args.cmd_index {
        do_index(args.flag_ref, args.arg_old_file, args.arg_index_file,
                 args.flag_blocksize)
    } else if args.cmd_delta {
        do_delta(args.arg_index_file, args.arg_new_file, args.arg_delta_file)
    } else {
        assert!(args.cmd_patch);
        do_patch(args.flag_ref,
                 args.arg_old_file, args.arg_delta_file, args.arg_new_file)
    };

    match result {
        Ok(()) => {},
        Err(e) => {
            write!(io::stderr(), "Fatal error: {}\n", e).is_ok(); // Ignore error
            process::exit(1);
        }
    }
}

/// Hashes files into a Hashes structure from an iterator of filenames.
fn hash_files<P: AsRef<Path>, I: Iterator<Item=P>>(filenames: I,
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
fn write_index(index: File, hashes: DefaultHashes) -> io::Result<()> {
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
fn read_index<R: Read>(index: R)
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

/// 'index' command: write the index file.
fn do_index(references: Vec<String>, old_file: String, index_file: String,
            blocksize: usize)
    -> io::Result<()>
{
    let index = try!(File::create(index_file));

    // Hash all the reference files
    let hashes = try!(hash_files([old_file].iter().chain(references.iter()),
                                 blocksize));

    // Write out the hashes
    write_index(index, hashes)
}

/// 'delta' command: write the delta file.
fn do_delta(index_file: String, new_file: String, delta_file: String)
    -> io::Result<()>
{
    let delta = try!(File::create(&delta_file));
    let (hashes, blocksize) = {
        let index = try!(File::open(&index_file));
        info!("Reading index file {}...", index_file);
        try!(read_index(index))
    };

    let mut file = io::BufReader::new(try!(File::open(new_file)));
    let mut pos: u64 = 0;

    let mut delta = io::BufWriter::new(delta);
    try!(delta.write_all(b"RS-SYNCD"));
    try!(delta.write_u16::<BigEndian>(0x0001)); // 0.1
    try!(delta.write_u32::<BigEndian>(blocksize as u32));
    try!(delta.write_u16::<BigEndian>(0)); // Single-file mode

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
        let get_sha1 = |pos: u64, block_start: u64, buffer: &[u8]|
            -> [u8; 20]
        {
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
                New(u64),
            }
            let mut sha1 = None;
            let mut match_what = Match::No;
            if let Some(sha1_hashes) = back_blocks.get(&adler32.hash()) {
                info!("Found backref Adler32 at position {}: {}",
                      pos, adler32.hash());
                sha1 = Some(get_sha1(pos, block_start, &buffer));
                if let Some(offset) = sha1_hashes.get(sha1.as_ref().unwrap()) {
                    info!("SHA-1 matches; old position: {}", offset);
                    match_what = Match::New(offset.clone());
                }
            }
            if let Match::No = match_what {
                if let Some(sha1_hashes) = hashes.get(&adler32.hash()) {
                    info!("Found known Adler32 at position {}: {}",
                          pos, adler32.hash());
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
                        try!(copy(&mut file, &mut delta,
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
                        try!(copy(&mut file, &mut delta,
                                  CopyMode::Exact(len)));
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
                }
            }

            if read == blocksize &&
                (pos - block_start) as usize % blocksize == 0
            {
                let adler32 = adler32.hash();
                let sha1 = get_sha1(pos, block_start, &buffer);
                let offset = pos - read as u64;
                info!("Storing back-ref to pos {}; Adler32: {}, SHA-1: {}",
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
                        try!(copy(&mut file, &mut delta, CopyMode::Exact(len)));
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

/// 'patch' command: update the old file to get the new file.
fn do_patch(references: Vec<String>,
            old_file: String, delta_file: String, new_file: String)
    -> io::Result<()>
{
    // Open the new file
    let mut file = try!(File::create(new_file));

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
    let hashes = try!(hash_files([old_file].iter().chain(references.iter()),
                                 blocksize));

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
