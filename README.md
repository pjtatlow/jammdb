# jammdb

## Just Another Memory Mapped Database

[![crates.io](https://img.shields.io/crates/v/jammdb?style=flat-square)](https://crates.io/crates/jammdb)
[![docs.rs](https://img.shields.io/badge/docs-latest-blue.svg?style=flat-square)](https://docs.rs/jammdb)
[![Build status](https://img.shields.io/github/actions/workflow/status/pjtatlow/jammdb/rust.yml?branch=master&style=flat-square)](https://github.com/pjtatlow/jammdb/actions/workflows/rust.yml)
[![Coverage Status](https://img.shields.io/codecov/c/gh/pjtatlow/jammdb?style=flat-square)](https://codecov.io/gh/pjtatlow/jammdb)
[![License](https://img.shields.io/crates/l/jammdb?style=flat-square)](https://crates.io/crates/jammdb)
[![Dependency Status](https://deps.rs/repo/github/pjtatlow/jammdb/status.svg?style=flat-square)](https://deps.rs/repo/github/pjtatlow/jammdb)

`jammdb` is an embedded, single-file database that allows you to store key /
value pairs as bytes.

It started life as a Rust port of
[Ben Johnson's](https://twitter.com/benbjohnson)
[BoltDB](https://github.com/boltdb/bolt), which was inspired by
[Howard Chu's](https://twitter.com/hyc_symas) [LMDB](http://symas.com/mdb/), so
please check out both of these awesome projects!

`jammdb` offers [ACID](https://en.wikipedia.org/wiki/ACID) compliance,
[serializable](https://en.wikipedia.org/wiki/Serializability) and
[isolated](https://en.wikipedia.org/wiki/Isolation_(database_systems))
transactions, with multiple lock-free readers and a single concurrent writer.
The data is organized in a
[single level](https://en.wikipedia.org/wiki/Single-level_store)
[B+ tree](https://en.wikipedia.org/wiki/B%2B_tree) so random and sequential
reads are very fast. The underlying file is
[memory mapped](https://en.wikipedia.org/wiki/Memory-mapped_file), so reads
require no additional memory allocation.

## Supported platforms

`jammdb` is continuously cross-compiled and tested on the following platforms:

- `x86_64-unknown-linux-gnu` (Linux)
- `i686-unknown-linux-gnu`
- `x86_64-unknown-linux-musl` (Linux MUSL)
- `x86_64-apple-darwin` (OSX)
- `x86_64-pc-windows-msvc` (Windows)
- `i686-pc-windows-msvc`
- `x86_64-pc-windows-gnu`
- `i686-pc-windows-gnu`

## Examples

Here are a couple of simple examples to get you started, but you should check
out the docs for more details.

### Simple put and get

```rust
use jammdb::{DB, Data, Error};

fn main() -> Result<(), Error> {
{
    // open a new database file
    let db = DB::open("my-database.db")?;

    // open a writable transaction so we can make changes
    let tx = db.tx(true)?;

    // create a bucket to store a map of first names to last names
    let names_bucket = tx.create_bucket("names")?;
    names_bucket.put("Kanan", "Jarrus")?;
    names_bucket.put("Ezra", "Bridger")?;

    // commit the changes so they are saved to disk
    tx.commit()?;
}
{
    // open the existing database file
    let db = DB::open("my-database.db")?;
    // open a read-only transaction to get the data
    let tx = db.tx(false)?;
    // get the bucket we created in the last transaction
    let names_bucket = tx.get_bucket("names")?;
    // get the key/ value pair we inserted into the bucket
    if let Some(data) = names_bucket.get("Kanan") {
        assert!(data.is_kv());
        assert_eq!(data.kv().value(), b"Jarrus");
    }
}
    Ok(())
}
```

### Storing structs

```rust
use jammdb::{DB, Data, Error};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize)]
struct User {
    username: String,
    password: String,
}

fn main() -> Result<(), Error> {
    let user = User{
        username: "my-user".to_string(),
        password: "my-password".to_string(),
    };
{
    // open a new database file and start a writable transaction
    let db = DB::open("my-database.db")?;
    let tx = db.tx(true)?;

    // create a bucket to store users
    let users_bucket = tx.create_bucket("users")?;

    // serialize struct to bytes and store in bucket
    let user_bytes = rmp_serde::to_vec(&user).unwrap();
    users_bucket.put("user1", user_bytes)?;

    // commit the changes so they are saved to disk
    tx.commit()?;
}
{
    // open the existing database file
    let db = DB::open("my-database.db")?;
    // open a read-only transaction to get the data
    let tx = db.tx(false)?;
    // get the bucket we created in the last transaction
    let users_bucket = tx.get_bucket("users")?;
    // get the key / value pair we inserted into the bucket
    if let Some(data) = users_bucket.get("user1") {
        if data.is_kv() {
            let kv = data.kv();
            // deserialize into a user struct
            let db_user: User = rmp_serde::from_slice(kv.value()).unwrap();
            assert_eq!(db_user, user);
        }
    }
}
    Ok(())
}
```

## MSRV

Currently `1.63`

## License

Available under both the [Apache License](LICENSE-APACHE) or the
[MIT license](LICENSE-MIT).
