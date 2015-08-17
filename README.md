What is this?
=============

This is an rsync-clone written in the [Rust](https://www.rust-lang.org/) programming language. It will contain a library, allowing to efficiently synchronize related files using delta encoding; an rdiff clone, allowing to generate binary patches of large files offline; and a rsync clone, updating files efficiently over the network.

Current status
==============

This is very early work, which I am doing mainly to learn Rust.

I am currently working on the internals and on the rdiff clone, online synchronization over SSH to be done afterwards.
