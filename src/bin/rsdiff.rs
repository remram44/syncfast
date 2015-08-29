extern crate docopt;
extern crate env_logger;
#[macro_use] extern crate log;
extern crate rs_sync;
extern crate rustc_serialize;

use std::fs::File;
use std::io::{self, Write};
use std::path::Path;
use std::process;

use docopt::Docopt;
use rs_sync::index::{hash_files, read_index_file, write_index_file};
use rs_sync::delta::write_delta_file_single;
use rs_sync::patch::apply_diff;

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

/// 'index' command: write the index file.
pub fn do_index(references: Vec<String>, old_file: String, index_file: String,
                blocksize: usize)
    -> io::Result<()>
{
    let index = try!(File::create(index_file));

    // Hash all the reference files
    let hashes = try!(hash_files([old_file].iter().chain(references.iter()),
                                 blocksize));

    // Write out the hashes
    write_index_file(index, hashes)
}

/// 'delta' command: write the delta file.
pub fn do_delta(index_file: String, new_file: String, delta_file: String)
    -> io::Result<()>
{
    let mut delta = io::BufWriter::new(try!(File::create(&delta_file)));
    let (hashes, blocksize) = {
        let index = try!(File::open(&index_file));
        info!("Reading index file {}...", index_file);
        try!(read_index_file(index))
    };

    let file = io::BufReader::new(try!(File::open(new_file)));
    write_delta_file_single(&hashes, file, &mut delta, blocksize)
}

/// 'patch' command: update the old file to get the new file.
fn do_patch(references: Vec<String>, old_file: String,
            delta_file: String, new_file: String)
    -> io::Result<()>
{
    let references: Vec<&Path> = references.iter().map(|p| Path::new(p)).collect();
    apply_diff(references.into_iter(), Path::new(&old_file),
               Path::new(&delta_file), Path::new(&new_file))
}
