## largetable

[![CircleCI](https://circleci.com/gh/colin353/largetable/tree/master.svg?style=shield)](https://circleci.com/gh/colin353/largetable)

[![codecov](https://codecov.io/gh/colin353/largetable/branch/master/graph/badge.svg)](https://codecov.io/gh/colin353/largetable)

This is mostly just a project for me to learn Rust. The goal is to create
a database, loosely modeled after BigTable. It's mostly a 2D key-value store
which doesn't support complicated queries. I'm mostly making things
up as I go along, so there could be some pretty bad problems, which I'm
hoping to work out as they arise.

## Todo list

Things I'd like to do pretty soon:

- [x] Commit log
- [x] Actual timestamps used in MUpdates, and use them for serving selects
- [x] Measure code coverage with kcov
- [ ] Merging multiple DTables together into a single table
- [ ] Automatically detecting when MTable is too large
- [ ] Automatic minor compactions
- [ ] Automatic major compaction
- [ ] Row and column deletion queries

Medium term ideas:

- [ ] Support for multithreaded operations
- [ ] Performance testing with larger volumes of data
- [ ] Compare performance with existing database systems
- [ ] Flame graphs and checking where bottlenecks are
- [ ] See how varying the number of workers changes perf
- [ ] Handle selects with a fixed timestamp
- [ ] Garbage collect historical data (perhaps when merging DTables?)

Long term things that would be interesting to do:

- [ ] Clustering support
- [ ] Method for building docker images
- [ ] Schemas? Joins? Complicated queries?

## Overview

Data is stored in two possible places, either a 2D mutable sorted map in memory (memtable or MTable), or a 2D immutable sorted map on disk (disktable or DTable). Writes are applied to the memtable. Reads run against both the memtable and the disktables in parallel, and the results are merged.

Writes to the memtable are followed by a write to the commit log. When the server comes online, it reads the commit log back into memory.

Eventually, after many writes, the memtable may grow until it is too large. At that point, it is written to disk in the form of a DTable (a "minor compaction") and the commit log is truncated.

Although the server may read from many DTables, reads are more efficient on a small number of large DTables than a large number of small DTables. DTables are merged together once in a while to keep the number of DTables from getting too large (a "major compaction").

## Building

First, create the protobuf generated code with:

  protoc --rust_out src/generated src/protobuf/dtable.proto

Now, you actually have to fix some of the generated code, because it
actually doesn't compile correctly without a few type annotations. You'll get
an error like this:

  error[E0282]: unable to infer enough type information about `T`
  --> src/generated/dtable.rs:143:26

That's fine, just go into that line and convert from

  if self.value != ::std::vec::Vec::new() {

to this:

  if self.value != ::std::vec::Vec::<u8>::new() {

you might have to do it a few times.

There are actually two binaries in here: one is a CLI-based client, and
one is a server. To build the server, do:

  cargo build --bin largetable

For the CLI interface, you can do:

  cargo build --bin largetable-cli

## Testing

Same as above, try using `cargo test` with either `--bin largetable` or `--bin largetable-cli`.
