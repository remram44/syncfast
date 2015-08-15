extern crate adler32;
#[macro_use] extern crate log;
extern crate sha1;

mod hasher;
pub mod utils;

pub use hasher::{DefaultHashes, Hashes};
