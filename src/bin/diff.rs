extern crate adler32;
extern crate byteorder;
extern crate docopt;
extern crate env_logger;
#[macro_use] extern crate log;
extern crate rs_sync;
extern crate rustc_serialize;
extern crate sha1;

use std::collections::{HashMap, HashSet};
use std::default::Default;
use std::fs::File;
use std::io::{self, Read, Seek, Write};
use std::path::Path;
use std::process;

use adler32::RollingAdler32;
use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};
use docopt::Docopt;
use log::LogLevel;
use rs_sync::{Adler32_SHA1, DefaultHashes};
use rs_sync::utils::{copy, CopyMode, ReadRetry, to_hex};
use sha1::Sha1;

static USAGE: &'static str = "
rdiff clone.

Usage:
  rs-diff index [--ref=<ref_file>]... <old-file> <index-file>
  rs-diff delta <index-file> <new-file> <delta-file>
  rs-diff patch [--ref=<ref>] <old-file> <delta-file> <new-file>
  rs-diff (-h | --help)
  rs-diff --version 

Options:
  -h --help     Show this screen.
";

#[derive(RustcDecodable)]
struct Args {
    cmd_index: bool,
    cmd_delta: bool,
    cmd_patch: bool,
    flag_ref: Vec<String>,
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
        do_index(args.flag_ref, args.arg_old_file, args.arg_index_file)
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

/// 'index' command: write the index file.
fn do_index(references: Vec<String>,
            old_file: String, index_file: String)
    -> io::Result<()>
{
    let index = try!(File::create(index_file));

    // Hash all the reference files
    let hashes = try!(hash_files([old_file].iter().chain(references.iter())));

    // Write out the hashes
    write_index(index, hashes)
}

fn hash_files<'a, I: Iterator<Item=&'a String>>(filenames: I)
    -> io::Result<DefaultHashes>
{
    let mut hashes: DefaultHashes = Default::default();
    for filename in filenames {
        let path = Path::new(&filename).to_owned();
        info!("Indexing {}", path.to_string_lossy());
        let f = try!(File::open(&path));
        try!(hashes.hash(path, f));
    }
    Ok(hashes)
}

fn write_index(index: File, hashes: DefaultHashes) -> io::Result<()> {
    info!("Writing index file: {} hashes", hashes.blocks().len());
    let mut index = io::BufWriter::new(index);
    try!(index.write_all(b"RS-SYNCI"));
    try!(index.write_u16::<BigEndian>(0x0001)); // 0.1
    try!(index.write_u32::<BigEndian>(hashes.blocks().len() as u32));
    for h in hashes.blocks().keys() {
        try!(index.write_u32::<BigEndian>(h.adler32));
        try!(index.write_all(&h.sha1));
    }
    Ok(())
}

fn read<'a, R: Read>(file: &mut R, buffer: &'a mut [u8], size: usize)
    -> io::Result<&'a [u8]>
{
    if try!(file.read_retry(&mut buffer[..size])) != size {
        return Err(io::Error::new(io::ErrorKind::InvalidData,
                                  "Unexpected end of file"));
    }
    Ok(&buffer[..size])
}

fn read_index<R: Read>(index: R)
    -> io::Result<HashMap<u32, HashSet<[u8; 20]>>>
{
    let mut hashes: HashMap<u32, HashSet<[u8; 20]>> = HashMap::new();
    let mut index = io::BufReader::new(index);
    let mut buffer: [u8; 8] = unsafe { ::std::mem::uninitialized() };
    if try!(read(&mut index, &mut buffer, 8)) != b"RS-SYNCI" {
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
    let nb_hashes = try!(index.read_u32::<BigEndian>());
    info!("Index file is version {}.{}", version >> 8, version & 0xFF);
    for _ in 0..nb_hashes {
        let adler32 = try!(index.read_u32::<BigEndian>());
        info!("Read Adler32: {}", adler32);
        let mut sha1: [u8; 20] = unsafe { ::std::mem::uninitialized() };
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
            None => {
                true
            }
        } {
            let mut set = HashSet::new();
            set.insert(sha1);
            assert!(hashes.insert(adler32, set).is_none());
        }
    }
    if try!(index.read(&mut [0u8])) != 0 {
        return Err(io::Error::new(io::ErrorKind::InvalidData,
                                  "Trailing data at end of file"));
    }
    Ok(hashes)
}

/// 'delta' command: write the delta file.
fn do_delta(index_file: String, new_file: String, delta_file: String)
    -> io::Result<()>
{
    let delta = try!(File::create(&delta_file));
    let hashes = {
        let index = try!(File::open(&index_file));
        info!("Reading index file {}...", index_file);
        try!(read_index(index))
    };

    let mut file = io::BufReader::new(try!(File::open(new_file)));
    let mut pos: u64 = 0;

    let mut delta = io::BufWriter::new(delta);
    try!(delta.write_all(b"RS-SYNCD"));
    try!(delta.write_u16::<BigEndian>(0x0001)); // 0.1

    // Reads the file by blocks
    loop {
        let block_start = pos;
        info!("Starting scan at position {}", pos);

        // Reads max 4096 bytes
        let mut buffer: [u8; 4096] = unsafe { ::std::mem::uninitialized() };
        let read = try!(file.read_retry(&mut buffer));
        if read == 0 {
            return Ok(());
        }
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
                                   - read as usize) % 4096;
                    let mut hasher = Sha1::new();
                    if read == 4096 {
                        hasher.update(&buffer[buf_pos..]);
                        hasher.update(&buffer[..buf_pos]);
                    } else {
                        assert!(buf_pos == 0);
                        hasher.update(&buffer[..read]);
                    }
                    let mut digest: [u8; 20] = unsafe {
                        ::std::mem::uninitialized()
                    };
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
                        try!(copy(&mut file, &mut delta,
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
                try!(copy(&mut file, &mut delta, CopyMode::Exact(len)));
                try!(file.seek(io::SeekFrom::Start(pos)));
                break;
            }

            {
                let idx = (pos % 4096) as usize;
                adler32.remove(4096, buffer[idx]);
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
    // Hash all the reference files
    let hashes = try!(hash_files([old_file].iter().chain(references.iter())));

    // Open the new file
    let mut file = try!(File::create(new_file));

    // Read the delta file
    let mut delta = io::BufReader::new(try!(File::open(delta_file)));
    let mut buffer: [u8; 8] = unsafe { ::std::mem::uninitialized() };
    if try!(read(&mut delta, &mut buffer, 8)) != b"RS-SYNCD" {
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

    loop {
        match delta.read_u8() {
            Ok(0x01) => { // LITERAL
                info!("Literal block");
                let len = try!(delta.read_u16::<BigEndian>()) as usize + 1;
                info!("Size: {}", len);
                try!(copy(&mut delta, &mut file, CopyMode::Exact(len)));
            }
            Ok(0x02) => { // KNOWN_BLOCK
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
                    let mut buf: [u8; 20] = unsafe {
                        ::std::mem::uninitialized()
                    };
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
                                               CopyMode::Maximum(4096)));
                        info!("Copied {} bytes", copied);
                    }
                }
            }
            Ok(c) => {
                error!("Invalid command {:02X}", c);
                return Err(io::Error::new(io::ErrorKind::InvalidData,
                                          "Invalid delta command"));
            }
            Err(byteorder::Error::UnexpectedEOF) => break,
            Err(byteorder::Error::Io(e)) => return Err(e),
        }
    }

    Ok(())
}
