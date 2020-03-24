[![Coverage Status](https://codecov.io/gh/pjtatlow/jammdb/branch/master/graph/badge.svg)](https://codecov.io/gh/pjtatlow/jammdb)

# jammdb

## Just Another Memory Mapped Database

jammdb is an embedded, single-file database that allows you to store key-value pairs as bytes.

It is heavily inspired by [Ben Johnson's](https://twitter.com/benbjohnson) awesome [BoltDB](https://github.com/boltdb/bolt),
which was inspired by [Howard Chu's](https://twitter.com/hyc_symas) [LMDB](http://symas.com/mdb/),
so please check out both of these awesome projects!

jammdb offers
[ACID](https://en.wikipedia.org/wiki/ACID) compliance,
[serializable](https://en.wikipedia.org/wiki/Serializability) and
[isolated](https://en.wikipedia.org/wiki/Isolation_(database_systems)) transactions,
with multiple lock-free readers and a single concurrent writer. The data is organized in a
[single level](https://en.wikipedia.org/wiki/Single-level_store) [B+ tree](https://en.wikipedia.org/wiki/B%2B_tree)
so random and sequential reads are very fast. The underlying file is memory mapped, so reads require no additional memory allocation.

This project is still in the very early stages, but detailed examples are coming soon!
