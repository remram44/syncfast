use std::collections::HashMap;
use std::default::Default;
use std::hash::Hash;
use std::io;

use adler32::adler32;
use sha1::Sha1;

pub struct BlockLocation<F> {
    pub file: F,
    pub offset: u64,
}

pub struct Hashes<F, H, HF: Fn(&[u8]) -> H> where F: Clone, H: Eq + Hash {
    blocks: HashMap<H, BlockLocation<F>>,
    hasher: HF,
}

impl<F, H, HF: Fn(&[u8]) -> H> Hashes<F, H, HF> where F: Clone, H: Eq + Hash {
    pub fn new(hasher: HF) -> Hashes<F, H, HF> {
        Hashes { blocks: HashMap::new(), hasher: hasher }
    }

    pub fn hash<R: io::Read>(&mut self, file: F, mut reader: R)
        -> io::Result<()>
    {
        let mut buffer: [u8; 4096] = unsafe { ::std::mem::uninitialized() };
        let mut offset = 0;
        loop {
            let r = reader.read(&mut buffer);
            match r {
                Err(e) => {
                    if e.kind() == io::ErrorKind::Interrupted {
                        continue;
                    } else {
                        return Err(e);
                    }
                }
                Ok(0) => break,
                Ok(n) => {
                    let hash = (self.hasher)(&buffer);
                    let loc = BlockLocation { file: file.clone(),
                                              offset: offset };
                    self.blocks.insert(hash, loc);
                    offset += n as u64;
                }
            }
        }
        Ok(())
    }

    pub fn find(&self, hash: &H) -> Option<&BlockLocation<F>> {
        self.blocks.get(hash)
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
    Adler32_SHA1 { adler32: adler32,
                   sha1: sha1 }
}

pub type DefaultHashes = Hashes<::std::path::PathBuf, Adler32_SHA1,
                                fn(&[u8]) -> Adler32_SHA1>;

impl Default for DefaultHashes {
    fn default() -> DefaultHashes {
        DefaultHashes::new(adler32_sha1)
    }
}
