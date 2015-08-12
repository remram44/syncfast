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
use std::io::{self, Read, Write};
use std::path::{Component, Path};
use std::process;

use adler32::RollingAdler32;
use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};
use docopt::Docopt;
use rs_sync::DefaultHashes;
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
            write!(io::stderr(), "Fatal error: {}", e);
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

fn copy<R: Read, W: Write>(reader: &mut R, writer: &mut W, size: usize)
    -> io::Result<()>
{
    unimplemented!();
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

    let mut delta = io::BufWriter::new(delta);
    try!(delta.write_all(b"RS-SYNCD"));
    try!(delta.write_u16::<BigEndian>(0x0001)); // 0.1

    let mut pos: u64 = 0;
    let mut block_start: u64 = 0;

    // Read the file one byte at a time, updating the Adler32 hash
    let mut adler32 = RollingAdler32::new(4096);
    loop {
        let mut buf = [0u8];
        match file.read(&mut buf) {
            Err(e) => {
                if e.kind() == io::ErrorKind::Interrupted {
                    continue;
                } else {
                    return Err(e);
                }
            }
            Ok(0) => break,
            Ok(_) => {}
        }
        pos += 1;
        adler32.append(buf[0]);

        // If we have a match
        if let Some(sha1_hashes) = hashes.get(&adler32.hash()) {
            // Compute the SHA-1
            let sha1 = {
                let mut hasher = Sha1::new();
                hasher.update(&adler32.buffer());
                let mut digest: [u8; 20] = unsafe {
                    ::std::mem::uninitialized()
                };
                hasher.output(&mut digest);
                digest
            };

            if sha1_hashes.contains(&sha1) {
                // Write previous block
                if pos - 4096 > block_start {
                    let len = pos - 4096 - block_start;
                    try!(delta.write_u8(0x01)); // LITERAL
                    try!(delta.write_u16::<BigEndian>(len as u16));
                    try!(file.seek(io::SeekFrom::Start(block_start)));
                    try!(copy(file, delta, len));
                    try!(file.seek(io::SeekFrom::Start(pos)));
                }

                // Write current block
                try!(delta.write_u8(0x02)); // KNOWN_BLOCK
                try!(delta.write_u32::<BigEndian>(adler32.hash()));
                try!(delta.write_all(&sha1));

                block_start = pos;

                // Advance by 4096 bytes
                adler32 = RollingAdler32::new();
            }
        }
    }

    unimplemented!();
}

/// 'patch' command: update the old file to get the new file.
fn do_patch(references: Vec<String>,
            old_file: String, delta_file: String, new_file: String)
    -> io::Result<()>
{
    unimplemented!();
}
