// This module exists to allow us to write hidden compile_fail doc tests that assert our types have the appropriate lifetimes.

/// // Make sure a tx cannot outlife a db.
/// ```compile_fail
/// use jammdb::{DB, Tx, Error};
///
/// fn main() -> Result<(), Error> {
///     let tx: Tx;
///     {
///         // open a new database file
///         let db = DB::open("my-database.db")?;
///
///         // open a writable transaction so we can make changes
///         tx = db.tx(true)?;
///     }
///     let names_bucket = tx.get_bucket("names")?;
///     Ok(())
/// }
///
/// ```
///
#[doc(hidden)]
#[allow(dead_code)]
struct TxLifetime();

/// // Make sure a bucket cannot outlife a tx.
/// ```compile_fail
/// use jammdb::{DB, Bucket, Error};
///
/// fn main() -> Result<(), Error> {
///     // open a new database file
///     let db = DB::open("my-database.db")?;
///     let b: Bucket;
///     {
///         // open a writable transaction so we can make changes
///         let tx = db.tx(true)?;
///         b = tx.get_bucket("names")?;
///     }
///     b.put("abc", "def");
///     Ok(())
/// }
///
/// ```
///
#[doc(hidden)]
#[allow(dead_code)]
struct BucketLifetime();

/// // Make sure a kv-pair cannot outlive a tx.
/// ```compile_fail
/// use jammdb::{DB, KVPair, Error};
///
/// fn main() -> Result<(), Error> {
///     // open a new database file
///     let db = DB::open("my-database.db")?;
///     let kv: KVPair;
///     {
///         // open a writable transaction so we can make changes
///         let tx = db.tx(true)?;
///         let b = tx.get_bucket("names")?;
///         kv = b.get_kv("data").unwrap();
///     }
///     let key = kv.key();
///     Ok(())
/// }
/// ```
///
#[doc(hidden)]
#[allow(dead_code)]
struct KVPairLifetime();
