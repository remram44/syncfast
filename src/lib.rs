extern crate adler32;
extern crate byteorder;
#[macro_use] extern crate log;
extern crate sha1;

mod hasher;
pub mod utils;
pub mod diff;

pub use hasher::{Adler32_SHA1, adler32_sha1, DefaultHashes, Hashes};
