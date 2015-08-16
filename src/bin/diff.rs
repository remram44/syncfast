extern crate adler32;
extern crate byteorder;
extern crate docopt;
extern crate env_logger;
#[macro_use] extern crate log;
extern crate rs_sync;
extern crate rustc_serialize;
extern crate sha1;

use std::cmp::min;
use std::collections::{HashMap, HashSet};
use std::default::Default;
use std::fs::File;
use std::io::{self, Read, Seek, Write};
use std::path::{Component, Path};
use std::process;

use adler32::RollingAdler32;
use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};
use docopt::Docopt;
use rs_sync::DefaultHashes;
use rs_sync::utils::ReadRetry;
use sha1::Sha1;

static USAGE: &'static str = "
rdiff clone.

Usage:
  rs-diff index [--reference=<ref_file>]... <old-file> <index-file>
  rs-diff delta <index_file> <new-file> <delta-file>
  rs-diff patch [--reference=<ref>] <old-file> <delta-file> <new-file>
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
    flag_reference: Vec<String>,
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
        do_index(args.flag_reference,
                 args.arg_old_file, args.arg_index_file)
    } else if args.cmd_delta {
        do_delta(args.arg_index_file, args.arg_new_file, args.arg_delta_file)
    } else {
        assert!(args.cmd_patch);
        do_patch(args.flag_reference,
                 args.arg_old_file, args.arg_delta_file, args.arg_new_file)
    };

    match result {
        Ok(()) => {},
        Err(e) => {
            write!(io::stderr(), "Fatal error: {}", e).is_ok(); // Ignore error
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

    // Hash all that good stuff
    let mut hashes: DefaultHashes = Default::default();
    for filename in [old_file].iter().chain(references.iter()) {
        let path = Path::new(filename).to_owned();
        if !path.is_relative() {
            error!("One path is not relative");
            process::exit(1);
        }
        for comp in path.components() {
            if let Component::ParentDir = comp {
                error!("One path contains \"..\"");
                process::exit(1);
            }
        }
        info!("Indexing {}", path.to_string_lossy());
        let f = try!(File::open(&path));
        try!(hashes.hash(path, f));
    }

    // Write out the hashes
    info!("Writing index file: {} hashes", hashes.blocks().len());
    write_index(index, hashes)
}

fn write_index(index: File, hashes: DefaultHashes) -> io::Result<()> {
    let mut index = io::BufWriter::new(index);
    try!(index.write_all(b"RS-SYNCI"));
    try!(index.write_u16::<BigEndian>(0x0001)); // 0.1
    for h in hashes.blocks().keys() {
        try!(index.write_u32::<BigEndian>(h.adler32));
        try!(index.write_all(&h.sha1));
    }
    Ok(())
}

fn read<'a, R: Read>(file: &mut R, buffer: &'a mut [u8], size: usize)
    -> io::Result<&'a [u8]>
{
    if try!(file.read(&mut buffer[..size])) != size {
        return Err(io::Error::new(io::ErrorKind::InvalidData,
                                  "Unexpected end of file"));
    }
    Ok(&buffer[..size])
}

fn read_index(index: File) -> io::Result<HashMap<u32, HashSet<[u8; 20]>>> {
    let mut hashes = HashMap::new();
    let mut index = io::BufReader::new(index);
    let mut buffer: [u8; 8] = unsafe { ::std::mem::uninitialized() };
    if try!(read(&mut index, &mut buffer, 8)) != b"RS-SYNCI" {
        return Err(io::Error::new(io::ErrorKind::InvalidData,
                                  "Invalid index file"));
    }
    let version = try!(index.read_u16::<BigEndian>());
    if version != 0x0001 { // 0.1
        return Err(io::Error::new(io::ErrorKind::InvalidData,
                   format!("Index file in unknown version {}.{}",
                           version >> 8, version & 0xFF)));
    }
    loop {
        let adler32 = {
            let mut buf: [u8; 2] = unsafe { ::std::mem::uninitialized() };
            let len = try!(index.read(&mut buf));
            if len == 0 {
                return Ok(hashes);
            } else {
                assert!(len == 2);
                let mut cursor: io::Cursor<&[u8]> = io::Cursor::new(&buf);
                try!(cursor.read_u32::<BigEndian>())
            }
        };
        let mut sha1: [u8; 20] = unsafe { ::std::mem::uninitialized() };
        assert!(try!(index.read(&mut sha1)) == 20);

        if match hashes.get_mut(&adler32) {
            Some(set) => {
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
}

fn copy<R: Read, W: Write>(reader: &mut R, writer: &mut W, mut size: usize)
    -> io::Result<()>
{
    let mut buffer: [u8; 4096] = unsafe { ::std::mem::uninitialized() };
    while size > 0 {
        let len = min(4096, size);
        if try!(reader.read(&mut buffer[..len])) != len {
            return Err(io::Error::new(io::ErrorKind::Other,
                                      "Unexpected end of file"));
        }
        try!(writer.write_all(&buffer[..len]));
        size -= len;
    }
    Ok(())
}

/// 'delta' command: write the delta file.
fn do_delta(index_file: String, new_file: String, delta_file: String)
    -> io::Result<()>
{
    let delta = try!(File::create(delta_file));
    let hashes = {
        let index = try!(File::open(index_file));
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

        // Reads max 4096 bytes
        let mut buffer: [u8; 4096] = unsafe { ::std::mem::uninitialized() };
        let read = try!(file.read_retry(&mut buffer));
        pos += read as u64;

        // Hash it
        let mut adler32 = RollingAdler32::from_buffer(&buffer[..read]);

        // Now we advance while updating the Adler32 hash, until we find a
        // known block or we read 2**16 bytes
        loop {
            if let Some(sha1_hashes) = hashes.get(&adler32.hash()) {
                let sha1 = {
                    let mut hasher = Sha1::new();
                    hasher.update(&buffer[((pos % 4096) as usize)..]);
                    let mut digest: [u8; 20] = unsafe {
                        ::std::mem::uninitialized()
                    };
                    hasher.output(&mut digest);
                    digest
                };

                if sha1_hashes.contains(&sha1) {
                    // Write the unmatched part up to the known block
                    if (pos - block_start) as usize > read {
                        let len = (pos - block_start) as usize - read;
                        try!(delta.write_u8(0x01)); // LITERAL
                        try!(delta.write_u16::<BigEndian>((len - 1) as u16));
                        try!(file.seek(io::SeekFrom::Start(block_start)));
                        try!(copy(&mut file, &mut delta, len));
                        try!(file.seek(io::SeekFrom::Start(pos)));
                    }

                    // Write the reference to the known block
                    try!(delta.write_u8(0x02)); // KNOWN_BLOCK
                    try!(delta.write_u32::<BigEndian>(adler32.hash()));
                    try!(delta.write_all(&sha1));
                    break;
                }
            } else if (pos - block_start) as usize >= read + 65536 {
                // Write the whole block, so as to not overflow the u16 block
                // length field
                let len = (pos - block_start) as usize - read;
                try!(delta.write_u8(0x01)); // LITERAL
                try!(delta.write_u16::<BigEndian>(0xFFFF));
                try!(file.seek(io::SeekFrom::Start(block_start)));
                try!(copy(&mut file, &mut delta, len));
                try!(file.seek(io::SeekFrom::Start(pos)));
                break;
            }

            {
                let idx = (pos % 4096) as usize;
                adler32.remove(4096, buffer[idx]);
                if try!(file.read(&mut buffer[idx..idx + 1])) == 0 {
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
    unimplemented!();
}
