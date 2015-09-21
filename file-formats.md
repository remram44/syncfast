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
* Number of files for directory mode: 2 bytes; if 0, single-file mode
* For each file:
  * Filename (prefixed with length, 2 bytes) or "" in single-file mode
  * Blocks:
    * Either:
      * `\x01`, "LITERAL"
      * Block size - 1: 2 bytes (0 indicates 1-byte block, etc)
      * Block data verbatim
    * Or:
      * `\x02`, "KNOWN"
      * Adler32 hash, 4 bytes
      * SHA-1 hash, 20 bytes
    * Or:
      * `\x03`, "BACKREF"
      * File number where first reference occurred, 2 bytes
      * Offset in file where reference occurred, 8 bytes
    * Or:
      * `\x00`, "ENDFILE"
      * Indicates end of this file's blocks
  * Total size of file in bytes, 8 bytes
