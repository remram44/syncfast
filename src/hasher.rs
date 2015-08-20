use std::collections::HashMap;
use std::default::Default;
use std::hash::Hash;
use std::io;

use adler32::adler32;
use super::utils::{ReadRetry, to_hex};
use sha1::Sha1;

pub struct BlockLocation<F> {
    pub file: F,
    pub offset: u64,
}

pub struct Hashes<F, H, HF: Fn(&[u8]) -> H> where F: Clone, H: Eq + Hash {
    blocks: HashMap<H, BlockLocation<F>>,
    blocksize: usize,
    hasher: HF,
}

impl<F, H, HF: Fn(&[u8]) -> H> Hashes<F, H, HF> where F: Clone, H: Eq + Hash {
    pub fn new(hasher: HF, blocksize: usize) -> Hashes<F, H, HF> {
        Hashes { blocks: HashMap::new(), blocksize: blocksize, hasher: hasher }
    }

    pub fn hash<R: io::Read>(&mut self, file: F, mut reader: R)
        -> io::Result<()>
    {
        let mut buffer = vec![0u8; self.blocksize];
        let mut offset = 0;
        loop {
            let n = try!(reader.read_retry(&mut buffer));
            if n == 0 {
                break;
            }
            let hash = (self.hasher)(&buffer[..n]);
            let loc = BlockLocation { file: file.clone(),
                                      offset: offset };
            self.blocks.insert(hash, loc);
            offset += n as u64;
        }
        Ok(())
    }

    pub fn find(&self, hash: &H) -> Option<&BlockLocation<F>> {
        self.blocks.get(hash)
    }

    pub fn blocks(&self) -> &HashMap<H, BlockLocation<F>> {
        &self.blocks
    }

    pub fn blocksize(&self) -> usize {
        self.blocksize
    }
}

/// Default hashes, used in rs-sync: Adler32 and SHA-1
#[derive(PartialEq, Eq, Hash, Clone)]
pub struct Adler32_SHA1 {
    pub adler32: u32,
    pub sha1: [u8; 20],
}

/// Computes the default hashes, used in rs-sync: Adler32 and SHA-1
pub fn adler32_sha1(block: &[u8]) -> Adler32_SHA1 {
    let adler32 = adler32(block).unwrap();
    let sha1 = {
        let mut sha1 = Sha1::new();
        sha1.update(block);
        let mut digest: [u8; 20] = unsafe { ::std::mem::uninitialized() };
        sha1.output(&mut digest);
        digest
    };
    info!("Hash: size: {}, Adler32: {}, SHA-1: {}", block.len(),
          adler32, to_hex(&sha1));
    Adler32_SHA1 { adler32: adler32,
                   sha1: sha1 }
}

pub type DefaultHashes = Hashes<::std::path::PathBuf, Adler32_SHA1,
                                fn(&[u8]) -> Adler32_SHA1>;

impl Default for DefaultHashes {
    fn default() -> DefaultHashes {
        DefaultHashes::new(adler32_sha1, 4096)
    }
}
