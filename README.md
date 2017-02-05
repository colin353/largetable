## LargeTable

This is mostly just a project for me to learn Rust. The goal is to create
a database, loosely modeled after BigTable. It's mostly a 2D key-value store
which doesn't support complicated queries. I'm mostly making things
up as I go along, so there could be some pretty bad problems, which I'm
hoping to work out as they arise.

## Todo list

Things I'd like to do pretty soon:

- [ ] Support for multithreaded operations
- [ ] Merging MTables with DTables and saving them back to disk
- [ ] Automatically detecting when MTable is too large

Medium term ideas:

- [ ] Performance testing with larger volumes of data
- [ ] Compare performance with existing database systems
- [ ] Flame graphs and checking where bottlenecks are
- [ ] See how varying the number of workers changes perf

Long term things that would be interesting to do:

- [ ] Clustering support
- [ ] Method for building docker images
- [ ] Schemas? Joins? Complicated queries?

## Building

There are actually two binaries in here: one is a CLI-based client, and
one is a server. To build the server, do:

`cargo build --bin largetable`

For the CLI interface, you can do:

`cargo build --bin largetable-cli`

## Testing

Same as above, try using `cargo test` with either `--bin largetable` or `--bin largetable-cli`.
