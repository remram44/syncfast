use std::collections::HashMap;
use std::hash::Hash;
use std::io;

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
