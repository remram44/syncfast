use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::Path;

use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};
use log::Level;
use super::{adler32_sha1, DefaultHashes};
use utils::{ReadExt, to_hex};

/// Hashes files into a Hashes structure from an iterator of filenames.
pub fn hash_files<P: AsRef<Path>, I: Iterator<Item=P>>(filenames: I,
                                                       blocksize: usize)
    -> io::Result<DefaultHashes>
{
    info!("Creating index, blocksize = {}", blocksize);
    let mut hashes = DefaultHashes::new(adler32_sha1, blocksize);
    for filename in filenames {
        let path = filename.as_ref().to_owned();
        info!("Indexing {}", path.to_string_lossy());
        let f = try!(File::open(&path));
        try!(hashes.hash(path, f));
    }
    Ok(hashes)
}

/// Serializes a Hashes structure into an index file.
pub fn write_index_file<W: Write>(index: &mut W, hashes: &DefaultHashes)
    -> io::Result<()>
{
    info!("Writing index file: {} hashes", hashes.blocks().len());
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
        if log_enabled!(Level::Info) {
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
