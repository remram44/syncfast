extern crate rs_sync;
extern crate byteorder;

use std::collections::{HashMap, HashSet};
use std::io::{Cursor, Write};
use std::path::Path;

use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};
use rs_sync::{Adler32_SHA1, adler32_sha1, DefaultHashes};
use rs_sync::delta::write_delta_file_single;
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
    let blocks_found: HashSet<_> = index[18..].chunks(24)
                                              .map(|c| c.to_owned()).collect();
    let blocks_expected = [h_a, h_b, h_c, h_d, h_y, h_z];
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

#[test]
fn test_delta() {
    let new_file: &[u8] = b"aaaammmmsbbbbsmmmmZZ";
    let new_file = Cursor::new(new_file);
    let mut hashes = HashMap::new();
    for &(a, s) in [h_a, h_b, h_z].iter() {
        let mut set = HashSet::new();
        set.insert(s.to_owned());
        hashes.insert(a, set);
    }
    let mut delta: Vec<u8> = Vec::new();
    write_delta_file_single(&hashes, new_file, &mut delta, 4).unwrap();
    let mut expected = vec![
        // Magic
        b'R', b'S', b'-', b'S', b'Y', b'N', b'C', b'D',
        // Version
        0x00, 0x01,
        // Blocksize
        0x00, 0x00, 0x00, 0x04,
        // Single-file mode
        0x00, 0x00,
    ];
    // Known block
    expected.write_u8(0x02);
    expected.write_u32::<BigEndian>(h_a.0);
    expected.write(h_a.1);
    // Literal
    expected.write_u8(0x01);
    expected.write_u16::<BigEndian>(5 - 1);
    expected.extend(b"mmmms");
    // Known block
    expected.write_u8(0x02);
    expected.write_u32::<BigEndian>(h_b.0);
    expected.write(h_b.1);
    // Literal
    expected.write_u8(0x01);
    expected.write_u16::<BigEndian>(7 - 1);
    expected.extend(b"smmmmZZ");
    // TODO: Back reference here
    // TODO: should be a known block here
    /*// Known block
    expected.write_u8(0x01);
    expected.write_u32::<BigEndian>(h_z.0);
    expected.write(h_z.1);*/
    // End of file
    expected.write_u8(0x00);

    assert_eq!(delta, expected);
}

const h_a: (u32, &'static [u8; 20]) = (63832453,
    b"\x70\xc8\x81\xd4\xa2\x69\x84\xdd\xce\x79\
     \x5f\x6f\x71\x81\x7c\x9c\xf4\x48\x0e\x79"
);
const h_b: (u32, &'static [u8; 20]) = (64487817,
    b"\x8a\xed\x13\x22\xe5\x45\x0b\xad\xb0\x78\
     \xe1\xfb\x60\xa8\x17\xa1\xdf\x25\xa2\xca"
);
const h_c: (u32, &'static [u8; 20]) = (65143181,
    b"\x4b\xea\xad\x62\x92\xb7\xdb\x0f\x93\x54\
     \xe0\xd8\xb9\x15\xec\x0d\xbb\xc0\x3a\x5a"
);
const h_d: (u32, &'static [u8; 20]) = (65798545,
    b"\x01\x46\x4e\x16\x16\xe3\xfd\xd5\xc6\x0c\
     \x0c\xc5\x51\x6c\x1d\x14\x54\xcc\x41\x85"
);
const h_e: (u32, &'static [u8; 20]) = (66453909,
    b"\xb2\xc4\xee\x5d\xe8\x28\x66\xdb\x38\xf7\
     \x9c\x6d\x4a\x91\xa6\x26\x48\x6b\x70\xe9"
);
const h_f: (u32, &'static [u8; 20]) = (67109273,
    b"\xd3\x3f\xef\x58\xbe\xdd\x39\xdc\x1c\x2d\
     \x38\xf1\x63\x05\xb1\x00\x10\xe9\x05\x8e"
);
const h_z: (u32, &'static [u8; 20]) = (17825973,
    b"\xd3\x08\xe0\xb2\xd3\x6c\x5d\x24\x20\x86\
     \x9c\x6b\xf1\x12\xe3\x1e\x8d\x5b\x0d\x52"
);
const h_y: (u32, &'static [u8; 20]) = (17629363,
    b"\xe6\x2c\xdb\x27\xa6\x1e\x20\xb0\xf3\x0f\
     \x4f\xf4\x03\x47\x3f\xde\x44\x85\xa9\xba"
);
