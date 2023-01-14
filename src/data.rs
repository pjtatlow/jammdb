use std::marker::PhantomData;

use crate::bytes::Bytes;
use crate::node::Leaf;
use crate::ToBytes;

/// Key / Value or Bucket Data
///
/// The two enum variants represent either a key / value pair or a nested bucket.
/// If you want to access the underneath data, you must match the variant first.
///
/// # Examples
///
/// ```no_run
/// use jammdb::{DB, Data};
/// # use jammdb::Error;
///
/// # fn main() -> Result<(), Error> {
/// let db = DB::open("my.db")?;
/// let mut tx = db.tx(true)?;
/// let bucket = tx.create_bucket("my-bucket")?;
///
/// if let Some(data) = bucket.get("my-key") {
///     match data {
///         Data::Bucket(b) => assert_eq!(b.name(), b"my-key"),
///         Data::KeyValue(kv) => assert_eq!(kv.key(), b"my-key"),
///     }
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Data<'b, 'tx> {
    /// Contains data about a nested bucket
    Bucket(BucketName<'b, 'tx>),
    /// a key / value pair of bytes
    KeyValue(KVPair<'b, 'tx>),
}

impl<'b, 'tx> Data<'b, 'tx> {
    /// Checks if the `Data` is a `KVPair`
    pub fn is_kv(&self) -> bool {
        matches!(self, Data::KeyValue(_))
    }

    /// Asserts that the `Data` is a `KVPair` and returns the inner data
    ///
    /// Panics if the data is a Bucket.
    pub fn kv(&self) -> &KVPair {
        if let Self::KeyValue(kv) = self {
            return kv;
        }
        panic!("Cannot get KVPair from BucketData");
    }
}

impl<'b, 'tx> Into<Data<'b, 'tx>> for Leaf<'tx> {
    fn into(self) -> Data<'b, 'tx> {
        match self {
            Leaf::Bucket(name, _) => Data::Bucket(BucketName::new(name)),
            Leaf::Kv(key, value) => Data::KeyValue(KVPair::new(key, value)),
        }
    }
}

/// Nested bucket placeholder
///
/// This data type signifies that a given key is associated with a nested bucket.alloc
/// You can access the key using the `name` function.
/// The BucketData itself can be used to retreive the bucket using the `get_bucket` function.
///
/// # Examples
///
/// ```no_run
/// use jammdb::{DB, Data};
/// # use jammdb::Error;
///
/// # fn main() -> Result<(), Error> {
/// let db = DB::open("my.db")?;
/// let mut tx = db.tx(true)?;
/// let bucket = tx.create_bucket("my-bucket")?;
///
/// bucket.create_bucket("my-nested-bucket")?;
/// if let Some(data) = bucket.get("my-nested-bucket") {
///     if let Data::Bucket(b) = data {
///         let name: &[u8] = b.name();
///         assert_eq!(name, b"my-nested-bucket");
///         let nested_bucket = bucket.get_bucket(&b).unwrap();
///     }
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketName<'b, 'tx> {
    name: Bytes<'tx>,
    _phantom: PhantomData<&'b ()>,
}

impl<'b, 'tx> BucketName<'b, 'tx> {
    pub(crate) fn new(name: Bytes<'tx>) -> Self {
        BucketName {
            name,
            _phantom: PhantomData,
        }
    }

    /// Returns the name of the bucket as a byte slice.
    pub fn name(&self) -> &[u8] {
        self.name.as_ref()
    }
}

impl<'b, 'tx> ToBytes<'tx> for BucketName<'b, 'tx> {
    fn to_bytes(self) -> Bytes<'tx> {
        self.name
    }
}

impl<'b, 'tx> ToBytes<'tx> for &BucketName<'b, 'tx> {
    fn to_bytes(self) -> Bytes<'tx> {
        self.name.clone()
    }
}

/// Key / Value Pair
///
/// You can use the [`key`](#method.key) and [`value`](#method.value) methods to access the underlying byte slices.
/// The data is only valid for the life of the transaction,
/// so make a copy if you want to keep it around longer than that.
///
/// # Examples
///
/// ```no_run
/// use jammdb::{DB, Data};
/// # use jammdb::Error;
///
/// # fn main() -> Result<(), Error> {
/// let db = DB::open("my.db")?;
/// let mut tx = db.tx(false)?;
/// let bucket = tx.get_bucket("my-bucket")?;
///
/// // put a key / value pair into the bucket
/// bucket.put("my-key", "my-value")?;
/// if let Some(data) = bucket.get("my-key") {
///     if let Data::KeyValue(kv) = data {
///         let key: &[u8] = kv.key();
///         let value: &[u8] = kv.value();
///         assert_eq!(key, b"my-key");
///         assert_eq!(value, b"my-value");
///     }
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KVPair<'b, 'tx> {
    key: Bytes<'tx>,
    value: Bytes<'tx>,
    _phantom: PhantomData<&'b ()>,
}

impl<'b, 'tx> KVPair<'b, 'tx> {
    pub(crate) fn new(key: Bytes<'tx>, value: Bytes<'tx>) -> Self {
        KVPair {
            key,
            value,
            _phantom: PhantomData,
        }
    }

    /// Returns the key of the key / value pair as a byte slice.
    pub fn key(&self) -> &[u8] {
        self.key.as_ref()
    }

    /// Returns the value of the key / value pair as a byte slice.
    pub fn value(&self) -> &[u8] {
        self.value.as_ref()
    }

    /// Returns the key / value pair as a tuple of slices.
    pub fn kv(&self) -> (&[u8], &[u8]) {
        (self.key(), self.value())
    }
}

impl<'b, 'tx> Into<KVPair<'b, 'tx>> for (Bytes<'tx>, Bytes<'tx>) {
    fn into(self) -> KVPair<'b, 'tx> {
        KVPair::new(self.0, self.1)
    }
}

impl<'b, 'tx> Into<Option<KVPair<'b, 'tx>>> for Leaf<'tx> {
    fn into(self) -> Option<KVPair<'b, 'tx>> {
        match self {
            Self::Bucket(_, _) => None,
            Self::Kv(key, value) => Some(KVPair::new(key, value)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kv_pair() {
        let k = vec![1, 2, 3, 4];
        let v = vec![5, 6, 7, 8, 9, 0];

        let kv = KVPair::new(Bytes::Slice(&k), Bytes::Slice(&v));
        assert_eq!(kv.key(), &k[..]);
        assert_eq!(kv.value(), &v[..]);

        let kv = KVPair::new(Bytes::Slice(&k), Bytes::Slice(&v));
        assert_eq!(kv.key(), &k[..]);
        assert_eq!(kv.value(), &v[..]);
    }

    // #[test]
    // fn test_bucket_data() {
    //     let name = b"Hello Bucket!";
    //     let meta = BucketMeta {
    //         root_page: 3,
    //         next_int: 24_985_738_796,
    //     };

    //     let b = BucketData::new(Bytes::Slice(name), meta);
    //     assert_eq!(b.name(), name);
    //     assert_eq!(b.meta().root_page, meta.root_page);
    //     assert_eq!(b.meta().next_int, meta.next_int);
    //     assert_eq!(b.size(), 13 + std::mem::size_of_val(&meta));
    // }

    // #[test]
    // fn test_data() {
    //     let k = vec![1, 2, 3, 4, 5, 6, 7, 8];
    //     let v = vec![11, 22, 33, 44, 55, 66, 77, 88];

    //     let data: Data = Data::KeyValue(KVPair::new(Bytes::Slice(&k), Bytes::Slice(&v)));

    //     assert_eq!(data.node_type(), Node::TYPE_DATA);
    //     assert_eq!(data.key_bytes(), Bytes::Slice(&k));
    //     assert_eq!(data.key(), &k[..]);
    //     assert_eq!(data.value(), &v[..]);
    //     assert_eq!(data.size(), 16);

    //     let meta = BucketMeta {
    //         root_page: 456,
    //         next_int: 8_888_888,
    //     };
    //     let data: Data = Data::Bucket(BucketData::new(Bytes::Slice(&k), meta));

    //     assert_eq!(data.node_type(), Node::TYPE_BUCKET);
    //     assert_eq!(data.key_bytes(), Bytes::Slice(&k));
    //     assert_eq!(data.key(), &k[..]);
    //     assert_eq!(data.value(), meta.as_ref());
    //     assert_eq!(data.size(), 8 + std::mem::size_of_val(&meta));
    // }
}
