#[cfg(test)]
extern crate rand;

use std::io;

// largest prime smaller than 65536
const BASE: u32 = 65521;

// NMAX is the largest n such that 255n(n+1)/2 + (n+1)(BASE-1) <= 2^32-1
const NMAX: usize = 5552;

#[inline(always)]
fn DO1(adler: &mut u32, sum2: &mut u32, buf: &[u8]) {
    *adler += buf[0] as u32;
    *sum2 += *adler;
}

#[inline(always)]
fn DO2(adler: &mut u32, sum2: &mut u32, buf: &[u8]) {
    DO1(adler, sum2, &buf[0..1]);
    DO1(adler, sum2, &buf[1..2]);
}

#[inline(always)]
fn DO4(adler: &mut u32, sum2: &mut u32, buf: &[u8]) {
    DO2(adler, sum2, &buf[0..2]);
    DO2(adler, sum2, &buf[2..4]);
}

#[inline(always)]
fn DO8(adler: &mut u32, sum2: &mut u32, buf: &[u8]) {
    DO4(adler, sum2, &buf[0..4]);
    DO4(adler, sum2, &buf[4..8]);
}

#[inline(always)]
fn DO16(adler: &mut u32, sum2: &mut u32, buf: &[u8]) {
    DO8(adler, sum2, &buf[0..8]);
    DO8(adler, sum2, &buf[8..16]);
}

fn adler32<R: io::Read>(mut reader: R) -> io::Result<u32> {
    // initial Adler-32 value
    let mut adler: u32 = 1;
    let mut sum2: u32 = 0;

    let mut buf: [u8; NMAX] = unsafe { ::std::mem::uninitialized() };

    // do length NMAX blocks -- requires just one modulo operation
    let mut len = try!(reader.read(&mut buf));
    while len == NMAX {
        let mut pos = 0;
        while pos < NMAX {
            // 16 sums unrolled
            DO16(&mut adler, &mut sum2, &buf[pos..pos + 16]);
            pos += 16;
        }
        adler %= BASE;
        sum2 %= BASE;
        len = try!(reader.read(&mut buf));
    }

    // do remaining bytes (less than NMAX, still just one modulo)
    if len > 0 { // avoid modulos if none remaining
        let mut pos = 0;
        while len - pos >= 16 {
            DO16(&mut adler, &mut sum2, &buf[pos..pos + 16]);
            pos += 16;
        }
        while len - pos > 0 {
            adler += buf[pos] as u32;
            sum2 += adler;
            pos += 1;
        }
        adler %= BASE;
        sum2 %= BASE;
    }

    // return recombined sums
    Ok(adler | (sum2 << 16))
}

#[cfg(test)]
mod test {
    use rand;
    use rand::Rng;
    use std::io;

    use super::{BASE, adler32};

    fn adler32_slow<R: io::Read>(reader: R) -> io::Result<u32> {
        let mut a: u32 = 1;
        let mut b: u32 = 0;

        for byte in reader.bytes() {
            let byte = try!(byte) as u32;
            a = a.wrapping_add(byte) % BASE;
            b = b.wrapping_add(a) % BASE;
        }

        Ok((b << 16) | a)
    }

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
        let mut data = vec![0u8; 5589];
        for size in [0, 1, 3, 4, 5, 31, 32, 33, 67,
                     5550, 5552, 5553, 5568, 5584, 5589].iter().cloned() {
            rng.fill_bytes(&mut data[..size]);
            let r1 = io::Cursor::new(&data[..size]);
            let r2 = r1.clone();
            if adler32_slow(r1).unwrap() != adler32(r2).unwrap() {
                panic!("Comparison failed, size={}", size);
            }
        }
    }
}
