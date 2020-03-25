//! # Just Another Memory Mapped Database
//!
//! jammdb is an embedded, single-file database that allows you to store key-value pairs as bytes.
//!
//! It is heavily inspired by [Ben Johnson's](https://twitter.com/benbjohnson) awesome [BoltDB](https://github.com/boltdb/bolt),
//! which was inspired by [Howard Chu's](https://twitter.com/hyc_symas) [LMDB](http://symas.com/mdb/),
//! so please check out both of these awesome projects!
//!
//! jammdb offers
//! [ACID](https://en.wikipedia.org/wiki/ACID) compliance,
//! [serializable](https://en.wikipedia.org/wiki/Serializability) and
//! [isolated](https://en.wikipedia.org/wiki/Isolation_(database_systems)) transactions,
//! with multiple lock-free readers and a single concurrent writer. The data is organized in a
//! [single level](https://en.wikipedia.org/wiki/Single-level_store) [B+ tree](https://en.wikipedia.org/wiki/B%2B_tree)
//! so random and sequential reads are very fast. The underlying file is memory mapped, so reads require no additional memory allocation.
//!
//! This project is still in the very early stages, but detailed examples are coming soon!

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

	impl Drop for RandomFile {
		#[allow(unused_must_use)]
		fn drop(&mut self) {
			std::fs::remove_file(&self.path);
		}
	}
}
