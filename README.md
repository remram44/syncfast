[![Build Status](https://github.com/remram44/syncfast/workflows/Test/badge.svg)](https://github.com/remram44/syncfast/actions)
[![Crates.io](https://img.shields.io/crates/v/syncfast.svg)](https://crates.io/crates/syncfast)
[![Documentation](https://docs.rs/syncfast/badge.svg)](https://docs.rs/syncfast)
[![License](https://img.shields.io/crates/l/syncfast.svg)](https://github.com/remram44/syncfast/blob/master/LICENSE.txt)

What is this?
=============

This is an rsync clone written in the [Rust](https://www.rust-lang.org/) programming language. It is intended to provide the functionality of rsync, rdiff, and zsync in one single program, as well as some additions such as caching file signatures to make repeated synchronizations faster. It will also provide a library, allowing to use the functionality in your own programs.

Current status
==============

Core functionality is there. You can index and sync local folders, and sync over SSH.

The next step is implementing syncing over HTTP, and syncing "offline" (diff/patch).

How to use
==========

```
$ syncfast sync some/folder ssh://othermachine/home/folder
```

Notes
=====

The rsync algorithm: https://rsync.samba.org/tech_report/
How rsync works: https://rsync.samba.org/how-rsync-works.html

zsync: http://zsync.moria.org.uk/

Compression crate: https://crates.io/crates/flate2
