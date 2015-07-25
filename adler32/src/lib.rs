extern crate rand;

use std::io;
use rand::Rng;

const MOD_ADLER: u32 = 65521;

// TODO: Fast implementation
// zlib has one: https://github.com/madler/zlib/blob/master/adler32.c

pub fn adler32<R: io::Read>(reader: R) -> io::Result<u32> {
    let mut a: u32 = 1;
    let mut b: u32 = 0;

    for byte in reader.bytes() {
        let byte = try!(byte) as u32;
        a = a.wrapping_add(byte) % MOD_ADLER;
        b = b.wrapping_add(a) % MOD_ADLER;
    }

    Ok((b << 16) | a)
}

use self::adler32 as adler32_default;

#[test]
fn testvectors() {
    fn do_test(v: u32, bytes: &[u8]) {
        let r = io::Cursor::new(bytes);
        assert_eq!(adler32(r).unwrap(), v);
    }
    do_test(0x00000001, b"");
    do_test(0x00620062, b"a");
    do_test(0x024d0127, b"abc");
    do_test(0x29750586, b"message digest");
    do_test(0x90860b20, b"abcdefghijklmnopqrstuvwxyz");
    do_test(0x8adb150c, b"ABCDEFGHIJKLMNOPQRSTUVWXYZ\
                          abcdefghijklmnopqrstuvwxyz\
                          0123456789");
    do_test(0x97b61069, b"1234567890123456789012345678901234567890\
                          1234567890123456789012345678901234567890");
}

#[test]
fn compare() {
    let mut rng = rand::thread_rng();
    let mut data = vec![0u8; 128];
    for size in [0, 1, 3, 4, 5, 31, 32, 33, 67, 126].iter().cloned() {
        rng.fill_bytes(&mut data[..size]);
        let r1 = io::Cursor::new(&data[..size]);
        let r2 = r1.clone();
        assert_eq!(adler32_default(r1).unwrap(), adler32(r2).unwrap());
    }
}
