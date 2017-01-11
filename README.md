[![Build Status](https://travis-ci.org/remram44/rs-sync.svg?branch=master)](https://travis-ci.org/remram44/rs-sync/builds)

What is this?
=============

This is an rsync-clone written in the [Rust](https://www.rust-lang.org/) programming language. It will contain a library, allowing to efficiently synchronize related files using delta encoding; an rdiff clone, allowing to generate binary patches of large files offline; and a rsync clone, updating files efficiently over the network.

[Generated documentation](https://remram44.github.io/rs-sync/rs_sync/index.html)

Current status
==============

This is very early work, which I am doing mainly to learn Rust.

I am currently working on the internals and on the rdiff clone, online synchronization over SSH to be done afterwards.

Notes
=====

The rsync algorithm: https://rsync.samba.org/tech_report/
How rsync works: https://rsync.samba.org/how-rsync-works.html

Compression crate: https://crates.io/crates/flate2
