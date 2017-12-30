use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Read, Seek, Write};
use std::iter::once;
use std::path::{Path, PathBuf};

use byteorder::{ReadBytesExt, BigEndian};
use super::{Adler32_SHA1, DefaultHashes, adler32_sha1};
use utils::{copy, CopyMode, ReadExt, to_hex};

/// Apply the delta to a file to get the new file.
pub fn apply_diff<'a, I: Iterator<Item=&'a Path>, R: Read, W: Write>(
        references: I, old_file: &'a Path,
        delta: R, file: W)
    -> io::Result<()>
{
    let mut sources: HashMap<PathBuf, _> = HashMap::new();
    for filename in once(old_file).chain(references) {
        sources.insert(filename.to_path_buf(),
                       io::BufReader::new(try!(File::open(filename))));
    }
    apply_diff_map(sources, delta, file)
}

/// Apply the delta to a file to get the new file.
pub fn apply_diff_map<F: Read + Seek, R: Read, W: Write>(
        mut sources: HashMap<PathBuf, F>,
        mut delta: R, mut file: W)
    -> io::Result<()>
{
    // Read the delta file
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

    // Hash all the source files
    let mut hashes = DefaultHashes::new(adler32_sha1, blocksize);
    for (filename, mut file) in sources.iter_mut() {
        try!(hashes.hash(filename.clone(), &mut file));
    }

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
                let adler32 = try!(delta.read_u32::<BigEndian>());
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
                        let mut origin = sources.get_mut(&loc.file).expect("Got non-existing file from Hashes");
                        try!(origin.seek(io::SeekFrom::Start(loc.offset)));
                        let copied = try!(copy(&mut origin, &mut file,
                                               CopyMode::Maximum(blocksize)));
                        info!("Copied {} bytes", copied);
                    }
                }
            }
            0x03 => unimplemented!(),
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
