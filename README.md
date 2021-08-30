[![Build Status](https://github.com/remram44/syncfast/workflows/Test/badge.svg)](https://github.com/remram44/syncfast/actions)
[![Crates.io](https://img.shields.io/crates/v/syncfast.svg)](https://crates.io/crates/syncfast)
[![Documentation](https://docs.rs/syncfast/badge.svg)](https://docs.rs/syncfast)
[![License](https://img.shields.io/crates/l/syncfast.svg)](https://github.com/remram44/syncfast/blob/master/LICENSE.txt)

What is this?
=============

This is an rsync clone written in the [Rust](https://www.rust-lang.org/) programming language. It is intended to provide the functionality of rsync, rdiff, and zsync in one single program, as well as some additions such as caching file signatures to make repeated synchronizations faster. It will also provide a library, allowing to use the functionality in your own programs.

Current status
==============

Core functionality is there. You can index and sync local folders.

The next step is implementing SSH and HTTP remotes.

How to use
==========

Common options: `-X` indicates the location of the index file on the source side, and `-x` the index file on the destination side.

rsync
-----

```
$ syncfast sync some/folder othermachine:folder
```

Pre-computed indices are optional but make the operation faster:

```
$ syncfast index -x folder.idx some/folder
$ ssh othermachine \
  syncfast index -x folder.idx folder
$ syncfast sync -X folder.idx -x othermachine:folder.idx some/folder othermachine:folder
```

rdiff
-----

```
# Same as rdiff (signature/delta/patch)
$ syncfast index -x signature.idx old/folder
$ syncfast diff -o patch.bin -x signature.idx new/folder
$ syncfast patch old/folder patch.bin
```

zsync
-----

```
$ syncfast index -x data.tar.syncfast.idx data.tar
$ syncfast sync -X data.tar.syncfast.idx old/data.tar
# Or over network
$ syncfast sync -X http://example.org/data.tar.syncfast.idx old/data.tar
```

Notes
=====

The rsync algorithm: https://rsync.samba.org/tech_report/
How rsync works: https://rsync.samba.org/how-rsync-works.html

zsync: http://zsync.moria.org.uk/

Compression crate: https://crates.io/crates/flate2
