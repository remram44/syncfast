extern crate rs_sync;
extern crate byteorder;

use std::collections::HashSet;
use std::io::Cursor;
use std::path::Path;

use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};
use rs_sync::{Adler32_SHA1, adler32_sha1, DefaultHashes};
use rs_sync::index::write_index_file;

#[test]
fn test_index() {
    let mut hashes = DefaultHashes::new(adler32_sha1, 4);
    hashes.hash(Path::new("/file1").to_owned(),
                Cursor::new(b"aaaabbbbccccZZ" as &[u8]));
    hashes.hash(Path::new("/file2").to_owned(),
                Cursor::new(b"ddddbbbbYY" as &[u8]));
    let mut index = Vec::new();
    write_index_file(&mut index, &hashes);
    assert_eq!(index.len(), 18 + 6 * 24);
    assert_eq!(&index[..18], &[
        // Magic
        b'R', b'S', b'-', b'S', b'Y', b'N', b'C', b'I',
        // Version
        0x00, 0x01,
        // Blocksize
        0x00, 0x00, 0x00, 0x04,
        // Number of blocks
        0x00, 0x00, 0x00, 0x06,
    ]);
    let blocks_found: HashSet<_> = index[18..].chunks(24).map(|c| c.to_owned()).collect();
    let blocks_expected = [
        (63832453, b"\x70\xc8\x81\xd4\xa2\x69\x84\xdd\xce\x79\
                     \x5f\x6f\x71\x81\x7c\x9c\xf4\x48\x0e\x79"),
        (64487817, b"\x8a\xed\x13\x22\xe5\x45\x0b\xad\xb0\x78\
                     \xe1\xfb\x60\xa8\x17\xa1\xdf\x25\xa2\xca"),
        (65143181, b"\x4b\xea\xad\x62\x92\xb7\xdb\x0f\x93\x54\
                     \xe0\xd8\xb9\x15\xec\x0d\xbb\xc0\x3a\x5a"),
        (65798545, b"\x01\x46\x4e\x16\x16\xe3\xfd\xd5\xc6\x0c\
                     \x0c\xc5\x51\x6c\x1d\x14\x54\xcc\x41\x85"),
        (17825973, b"\xd3\x08\xe0\xb2\xd3\x6c\x5d\x24\x20\x86\
                     \x9c\x6b\xf1\x12\xe3\x1e\x8d\x5b\x0d\x52"),
        (17629363, b"\xe6\x2c\xdb\x27\xa6\x1e\x20\xb0\xf3\x0f\
                     \x4f\xf4\x03\x47\x3f\xde\x44\x85\xa9\xba"),
    ];
    let blocks_expected = blocks_expected.into_iter().map(|&(a, s)| {
        let mut vec = Vec::new();
        vec.write_u32::<BigEndian>(a).unwrap();
        vec.extend(s);
        vec
    }).collect::<HashSet<_>>();
    println!("{:?}", blocks_found);
    println!("{:?}", blocks_expected);
    assert!(blocks_found == blocks_expected);
}
