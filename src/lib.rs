//! # Just Another Memory Mapped Database
//!
//! jammdb is an embedded, single-file database that allows you to store key / value pairs as bytes.
//!
//! It started life as a Rust port of [Ben Johnson's](https://twitter.com/benbjohnson) awesome [BoltDB](https://github.com/boltdb/bolt),
//! which was inspired by [Howard Chu's](https://twitter.com/hyc_symas) [LMDB](http://symas.com/mdb/),
//! so please check out both of these awesome projects!
//!
//! jammdb offers
//! [ACID](https://en.wikipedia.org/wiki/ACID) compliance,
//! [serializable](https://en.wikipedia.org/wiki/Serializability) and
//! [isolated](https://en.wikipedia.org/wiki/Isolation_(database_systems)) transactions,
//! with multiple lock-free readers and a single concurrent writer. The data is organized in a
//! [single level](https://en.wikipedia.org/wiki/Single-level_store) [B+ tree](https://en.wikipedia.org/wiki/B%2B_tree)
//! so random and sequential reads are very fast. The underlying file is [memory mapped](https://en.wikipedia.org/wiki/Memory-mapped_file), so reads require no additional memory allocation.
//!
//! jammdb is meant to be very simple, and has only a few exported types. It will allow you to store data in collections (called [`Buckets`](struct.Bucket.html)),
//! and each bucket can contain any number of unique keys which map to either an arbitrary value (a `&[u8]`) or a nested bucket. Examples on how to use jammdb are below.
//! There are also more examples in the docs, be sure to check out
//! * Using a [`Cursor`] to iterate over the data in a bucket
//! * How to create and use multiple [`Transaction`]s
//! * Nested [`Buckets`](struct.Bucket.html)
//! * [`OpenOptions`](struct.OpenOptions.html) to provide parameters for opening a [`DB`](struct.DB.html)
//!
//! # Examples
//!
//! ## Simple put and get
//! ```no_run
//! use jammdb::{DB, Data, Error};
//!
//! fn main() -> Result<(), Error> {
//! {
//!     // open a new database file
//!     let mut db = DB::open("my-database.db")?;
//!
//!     // open a writable transaction so we can make changes
//!     let mut tx = db.tx(true)?;
//!
//!     // create a bucket to store a map of first names to last names
//!     let names_bucket = tx.create_bucket("names")?;
//!     names_bucket.put(b"Kanan", b"Jarrus")?;
//!     names_bucket.put(b"Ezra", b"Bridger")?;
//!
//!     // commit the changes so they are saved to disk
//!     tx.commit()?;
//! }
//! {
//!     // open the existing database file
//!     let mut db = DB::open("my-database.db")?;
//!     // open a read-only transaction to get the data
//!     let mut tx = db.tx(true)?;
//!     // get the bucket we created in the last transaction
//!     let names_bucket = tx.get_bucket("names")?;
//!     // get the key / value pair we inserted into the bucket
//!     if let Some(Data::KeyValue(kv)) = names_bucket.get(b"Kanan") {
//!         assert_eq!(kv.value(), b"Jarrus");
//!     }
//! }
//!     Ok(())
//! }
//! ```
//!
//! ## Storing structs
//! ```no_run
//! use jammdb::{DB, Data, Error};
//! use serde::{Deserialize, Serialize};
//! // use rmps crate to serialize structs using the MessagePack format
//! use rmp_serde::{Deserializer, Serializer};
//!
//! #[derive(Debug, PartialEq, Deserialize, Serialize)]
//! struct User {
//!     username: String,
//!     password: String,
//! }
//!
//! fn main() -> Result<(), Error> {
//!     let user = User{
//!         username: "my-user".to_string(),
//!         password: "my-password".to_string(),
//!     };
//! {
//!     // open a new database file and start a writable transaction
//!     let mut db = DB::open("my-database.db")?;
//!     let mut tx = db.tx(true)?;
//!
//!     // create a bucket to store users
//!     let users_bucket = tx.create_bucket("users")?;
//!
//!     // serialize struct to bytes and store in bucket
//!     let user_bytes = rmp_serde::to_vec(&user).unwrap();
//!     users_bucket.put(b"user1", user_bytes)?;
//!
//!     // commit the changes so they are saved to disk
//!     tx.commit()?;
//! }
//! {
//!     // open the existing database file
//!     let mut db = DB::open("my-database.db")?;
//!     // open a read-only transaction to get the data
//!     let mut tx = db.tx(true)?;
//!     // get the bucket we created in the last transaction
//!     let users_bucket = tx.get_bucket("users")?;
//!     // get the key / value pair we inserted into the bucket
//!     if let Some(Data::KeyValue(kv)) = users_bucket.get(b"user1") {
//!         // deserialize into a user struct
//!         let db_user: User = rmp_serde::from_slice(kv.value()).unwrap();
//!         assert_eq!(db_user, user);
//!     }
//! }
//!     Ok(())
//! }
//! ```
//!

#![warn(clippy::all)]
#![warn(missing_docs)]

mod bucket;
mod cursor;
mod data;
mod db;
mod errors;
mod freelist;
mod meta;
mod node;
mod page;
mod ptr;
mod transaction;

pub use bucket::Bucket;
pub use cursor::Cursor;
pub use data::*;
pub use db::{OpenOptions, DB};
pub use errors::*;
pub use transaction::Transaction;

#[cfg(test)]
mod testutil {
	use rand::{distributions::Alphanumeric, Rng};

	pub struct RandomFile {
		pub path: std::path::PathBuf,
	}

	impl RandomFile {
		pub fn new() -> RandomFile {
			loop {
				let filename: String = rand::thread_rng()
					.sample_iter(&Alphanumeric)
					.take(30)
					.collect();
				let path = std::env::temp_dir().join(filename);
				if path.metadata().is_err() {
					return RandomFile { path };
				}
			}
		}
	}

	impl AsRef<std::path::Path> for RandomFile {
		fn as_ref(&self) -> &std::path::Path {
			self.path.as_ref()
		}
	}

	impl Drop for RandomFile {
		#[allow(unused_must_use)]
		fn drop(&mut self) {
			std::fs::remove_file(&self.path);
		}
	}
}
