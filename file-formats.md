All numbers are big-endian.

# Index file

* Magic: 8 bytes `RS-SYNCI`
* Version: `\x00\x01`
* Blocksize: 4 bytes
* Number of hashes: 4 bytes
* For each hash:
  * Adler32: 4 bytes
  * SHA-1: 20 bytes

# Delta file

* Magic: 8 bytes `RS-SYNCD`
* Version: `\x00\x01`
* Blocksize: 4 bytes
* Blocks:
  * Either:
    * `\x01`, "LITERAL"
    * Block size - 1: 2 bytes (0 indicates 1-byte block, etc)
    * Block data verbatim
  * Or:
    * `\x02`, "KNOWN"
    * Adler32 hash, 4 bytes
    * SHA-1 hash, 20 bytes
