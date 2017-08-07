use std::fs::File;
use std::io::{self, Seek};
use std::iter::once;
use std::path::Path;

use byteorder::{self, ReadBytesExt, BigEndian};
use index::hash_files;
use super::Adler32_SHA1;
use utils::{copy, CopyMode, ReadExt, to_hex};

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
