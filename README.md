[![Build Status](https://travis-ci.org/remram44/rs-sync.svg?branch=master)](https://travis-ci.org/remram44/rs-sync/builds)
[![Say Thanks!](https://img.shields.io/badge/Say%20Thanks-!-1EAEDB.svg)](https://saythanks.io/to/remram44)

What is this?
=============

This is an rsync-clone written in the [Rust](https://www.rust-lang.org/) programming language. It is intended to provide the functionality provided by rsync, rdiff, and zsync in one single program, as well as some additions such as caching file signatures to make repeated synchronizations faster. It will also provide a library, allowing to use the functionality in your own programs.

[Generated documentation](https://remram44.github.io/rs-sync/rssync/index.html)

Current status
==============

I am still implementing core functionality. This is NOT ready for production use.

How to use
==========

Common options: `-X` indicates the location of the index file on the source side, and `-x` the index file on the destination side.

rsync
-----

```
$ rssync sync some/folder othermachine:folder
```

Pre-computed indices are optional but make the operation faster:

```
$ rssync index -x folder.idx some/folder
$ ssh othermachine \
  rssync index -x folder.idx folder
$ rssync sync -X folder.idx -x othermachine:folder.idx some/folder othermachine:folder
```

rdiff
-----

```
# Same as rdiff (signature/delta/patch)
$ rssync index -x signature.idx old/folder
$ rssync diff -o patch.bin -x signature.idx new/folder
$ rssync patch old/folder patch.bin
```

zsync
-----

```
$ rssync index -x data.tar.rssync.idx data.tar
$ rssync sync -X data.tar.rssync.idx old/data.tar
# Or over network
$ rssync sync -X http://example.org/data.tar.rssync.idx old/data.tar
```

Notes
=====

The rsync algorithm: https://rsync.samba.org/tech_report/
How rsync works: https://rsync.samba.org/how-rsync-works.html

zsync: http://zsync.moria.org.uk/

Compression crate: https://crates.io/crates/flate2
