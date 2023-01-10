use crate::bucket::{BucketMeta, META_SIZE};
use crate::bytes::Bytes;
use crate::node::{Node, NodeType};
use crate::page::LeafElement;

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
pub enum Data<'a> {
    /// Contains data about a nested bucket
    Bucket(BucketData<'a>),
    /// a key / value pair of bytes
    KeyValue(KVPair<'a>),
}

impl<'a> Data<'a> {
    pub(crate) fn from_leaf<'b>(l: &'b LeafElement) -> Data<'a> {
        match l.node_type {
            Node::TYPE_DATA => {
                Data::KeyValue(KVPair::new(Bytes::Slice(l.key()), Bytes::Slice(l.value())))
            }
            Node::TYPE_BUCKET => Data::Bucket(l.into()),
            _ => panic!("INVALID NODE TYPE"),
        }
    }

    pub(crate) fn node_type(&self) -> NodeType {
        match self {
            Data::Bucket(_) => Node::TYPE_BUCKET,
            Data::KeyValue(_) => Node::TYPE_DATA,
        }
    }

    pub(crate) fn key_parts<'b>(&'b self) -> Bytes<'a> {
        match self {
            Data::Bucket(b) => b.name.clone(),
            Data::KeyValue(kv) => kv.key.clone(),
        }
    }

    pub(crate) fn key(&self) -> &[u8] {
        match self {
            Data::Bucket(b) => b.name(),
            Data::KeyValue(kv) => kv.key(),
        }
    }

    pub(crate) fn value(&self) -> &[u8] {
        match self {
            Data::Bucket(b) => b.meta.as_ref(),
            Data::KeyValue(kv) => kv.value(),
        }
    }

    pub(crate) fn size(&self) -> usize {
        match self {
            Data::Bucket(b) => b.size(),
            Data::KeyValue(kv) => kv.size(),
        }
    }

    /// Checks if the `Data` is a `KVPair`
    pub fn is_kv(&self) -> bool {
        matches!(self, Data::KeyValue(_))
    }

    /// Asserts that the `Data` is a `KVPair` and returns the inner data
    ///
    /// This is an ergonomic function since data is wrapped up in a `Ref` and matching is annoying
    pub fn kv(&self) -> &KVPair {
        if let Self::KeyValue(kv) = self {
            return kv;
        }
        panic!("Cannot get KVPair from BucketData");
    }
}

/// Nested bucket placeholder
///
/// This data type signifies that a given key is associated with a nested bucket.alloc
/// You can access the key using the `name` function.
/// The bucket's name can be used to retreive the bucket using the `get_bucket` function.
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
///         let nested_bucket = bucket.get_bucket(b.name()).unwrap();
///     }
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketData<'a> {
    name: Bytes<'a>,
    meta: BucketMeta,
}

impl<'a> BucketData<'a> {
    pub(crate) fn new(name: Bytes<'a>, meta: BucketMeta) -> Self {
        BucketData { name, meta }
    }

    /// Returns the name of the bucket as a byte slice.
    pub fn name(&self) -> &[u8] {
        self.name.as_ref()
    }

    pub(crate) fn meta(&self) -> BucketMeta {
        self.meta
    }

    pub(crate) fn size(&self) -> usize {
        self.name.size() + META_SIZE
    }
}

impl<'a, 'b: 'a> From<&'a LeafElement> for BucketData<'b> {
    fn from(l: &'a LeafElement) -> Self {
        assert_eq!(
            l.node_type,
            Node::TYPE_BUCKET,
            "Cannot convert node_type {} to BucketData",
            l.node_type
        );
        let meta_bytes = l.value();
        let ptr = &meta_bytes[0] as *const u8;
        let meta = unsafe { *(ptr as *const BucketMeta) };
        BucketData::new(Bytes::Slice(l.key()), meta)
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
pub struct KVPair<'a> {
    key: Bytes<'a>,
    value: Bytes<'a>,
}

impl<'a> KVPair<'a> {
    pub(crate) fn new(key: Bytes<'a>, value: Bytes<'a>) -> Self {
        KVPair { key, value }
    }

    pub(crate) fn size(&self) -> usize {
        self.key.size() + self.value.size()
    }

    /// Returns the key of the key / value pair as a byte slice.
    pub fn key(&self) -> &[u8] {
        self.key.as_ref()
    }

    /// Returns the value of the key / value pair as a byte slice.
    pub fn value(&self) -> &[u8] {
        self.value.as_ref()
    }

    pub fn kv(&self) -> (&[u8], &[u8]) {
        (self.key(), self.value())
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
        assert_eq!(kv.size(), 10);

        let kv = KVPair::new(Bytes::Slice(&k), Bytes::Slice(&v));
        assert_eq!(kv.key(), &k[..]);
        assert_eq!(kv.value(), &v[..]);
        assert_eq!(kv.size(), 10);
    }

    #[test]
    fn test_bucket_data() {
        let name = b"Hello Bucket!";
        let meta = BucketMeta {
            root_page: 3,
            next_int: 24_985_738_796,
        };

        let b = BucketData::new(Bytes::Slice(name), meta);
        assert_eq!(b.name(), name);
        assert_eq!(b.meta().root_page, meta.root_page);
        assert_eq!(b.meta().next_int, meta.next_int);
        assert_eq!(b.size(), 13 + std::mem::size_of_val(&meta));
    }

    #[test]
    fn test_data() {
        let k = vec![1, 2, 3, 4, 5, 6, 7, 8];
        let v = vec![11, 22, 33, 44, 55, 66, 77, 88];

        let data: Data = Data::KeyValue(KVPair::new(Bytes::Slice(&k), Bytes::Slice(&v)));

        assert_eq!(data.node_type(), Node::TYPE_DATA);
        assert_eq!(data.key_parts(), Bytes::Slice(&k));
        assert_eq!(data.key(), &k[..]);
        assert_eq!(data.value(), &v[..]);
        assert_eq!(data.size(), 16);

        let meta = BucketMeta {
            root_page: 456,
            next_int: 8_888_888,
        };
        let data: Data = Data::Bucket(BucketData::new(Bytes::Slice(&k), meta));

        assert_eq!(data.node_type(), Node::TYPE_BUCKET);
        assert_eq!(data.key_parts(), Bytes::Slice(&k));
        assert_eq!(data.key(), &k[..]);
        assert_eq!(data.value(), meta.as_ref());
        assert_eq!(data.size(), 8 + std::mem::size_of_val(&meta));
    }
}
