use std::cmp::{Ord, Ordering};
use std::fmt;
use std::hash::{Hash, Hasher};

use crate::bucket::BucketMeta;
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
/// let mut db = DB::open("my.db")?;
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
#[derive(Clone, Debug, PartialEq)]
pub enum Data {
	/// Contains data about a nested bucket
	Bucket(BucketData),
	/// a key / value pair of bytes
	KeyValue(KVPair),
}

impl Data {
	pub(crate) fn from_leaf(l: &LeafElement) -> Data {
		match l.node_type {
			Node::TYPE_DATA => Data::KeyValue(KVPair::new(l.key(), l.value())),
			Node::TYPE_BUCKET => Data::Bucket(BucketData::new(l.key(), l.value())),
			_ => panic!("INVALID NODE TYPE"),
		}
	}

	pub(crate) fn node_type(&self) -> NodeType {
		match self {
			Data::Bucket(_) => Node::TYPE_BUCKET,
			Data::KeyValue(_) => Node::TYPE_DATA,
		}
	}

	pub(crate) fn key_parts(&self) -> SliceParts {
		match self {
			Data::Bucket(b) => b.name,
			Data::KeyValue(kv) => kv.key,
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
			Data::Bucket(b) => b.meta.slice(),
			Data::KeyValue(kv) => kv.value(),
		}
	}

	pub(crate) fn size(&self) -> u64 {
		match self {
			Data::Bucket(b) => b.size(),
			Data::KeyValue(kv) => kv.size(),
		}
	}

	pub(crate) fn is_kv(&self) -> bool {
		match self {
			Data::KeyValue(_) => true,
			_ => false,
		}
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
/// let mut db = DB::open("my.db")?;
/// let mut tx = db.tx(true)?;
/// let mut bucket = tx.create_bucket("my-bucket")?;
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
#[derive(Clone, Debug, PartialEq)]
pub struct BucketData {
	name: SliceParts,
	meta: SliceParts,
}

impl BucketData {
	pub(crate) fn new(name: &[u8], meta: &[u8]) -> Self {
		BucketData {
			name: SliceParts::from_slice(name),
			meta: SliceParts::from_slice(meta),
		}
	}

	pub(crate) fn from_slice_parts(name: SliceParts, meta: SliceParts) -> Self {
		BucketData { name, meta }
	}

	pub(crate) fn from_meta(name: SliceParts, meta: &BucketMeta) -> Self {
		BucketData {
			name,
			meta: SliceParts::from_slice(meta.as_ref()),
		}
	}

	/// Returns the name of the bucket as a byte slice.
	pub fn name(&self) -> &[u8] {
		self.name.slice()
	}

	pub(crate) fn meta(&self) -> BucketMeta {
		#[allow(clippy::cast_ptr_alignment)]
		unsafe {
			*(self.meta.ptr as *const BucketMeta)
		}
	}

	pub(crate) fn size(&self) -> u64 {
		self.name.size() + self.meta.size()
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
/// let mut db = DB::open("my.db")?;
/// let mut tx = db.tx(false)?;
/// let mut bucket = tx.get_bucket("my-bucket")?;
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
#[derive(Clone, Debug, PartialEq)]
pub struct KVPair {
	key: SliceParts,
	value: SliceParts,
}

impl KVPair {
	fn new(key: &[u8], value: &[u8]) -> Self {
		KVPair {
			key: SliceParts::from_slice(key),
			value: SliceParts::from_slice(value),
		}
	}

	pub(crate) fn from_slice_parts(key: SliceParts, value: SliceParts) -> Self {
		KVPair { key, value }
	}

	pub(crate) fn size(&self) -> u64 {
		self.key.size() + self.value.size()
	}

	/// Returns the key of the key / value pair as a byte slice.
	pub fn key(&self) -> &[u8] {
		self.key.slice()
	}

	/// Returns the value of the key / value pair as a byte slice.
	pub fn value(&self) -> &[u8] {
		self.value.slice()
	}
}

#[derive(Clone, Copy)]
pub(crate) struct SliceParts {
	ptr: *const u8,
	len: u64,
}

impl SliceParts {
	pub(crate) fn from_slice(s: &[u8]) -> SliceParts {
		let ptr;
		let len = s.len() as u64;
		if len > 0 {
			ptr = &s[0] as *const u8;
		} else {
			ptr = std::ptr::null::<u8>();
		}
		SliceParts { ptr, len }
	}

	pub(crate) fn slice(&self) -> &[u8] {
		unsafe { std::slice::from_raw_parts(self.ptr, self.len as usize) }
	}

	pub(crate) fn size(&self) -> u64 {
		self.len
	}
}

impl Ord for SliceParts {
	fn cmp(&self, other: &Self) -> Ordering {
		self.slice().cmp(other.slice())
	}
}

impl PartialOrd for SliceParts {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl PartialEq for SliceParts {
	fn eq(&self, other: &Self) -> bool {
		self.slice().eq(other.slice())
	}
}

impl Eq for SliceParts {}

impl Hash for SliceParts {
	fn hash<H: Hasher>(&self, state: &mut H) {
		self.slice().hash(state);
	}
}

impl fmt::Debug for SliceParts {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "[ ")?;
		for byte in self.slice() {
			write!(f, "{} ", byte)?
		}
		write!(f, "]")?;
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_slice_parts() {
		let s = vec![42, 84, 126];
		let s = s.as_slice();

		let parts = SliceParts::from_slice(s);
		assert_eq!(parts.slice(), s);
		assert_eq!(parts.size(), 3);

		assert_eq!(parts, SliceParts::from_slice(s));

		use std::collections::hash_map::RandomState;
		use std::hash::BuildHasher;
		let state = RandomState::new();
		let mut hasher_1 = state.build_hasher();
		let mut hasher_2 = state.build_hasher();
		parts.hash(&mut hasher_1);
		s.hash(&mut hasher_2);
		assert_eq!(hasher_1.finish(), hasher_2.finish());
		assert_eq!(format!("{:?}", s), "[42, 84, 126]");
		let s2 = vec![41, 85, 142];
		let other_parts = SliceParts::from_slice(s2.as_slice());
		assert_eq!(parts.partial_cmp(&other_parts), Some(Ordering::Greater));
		assert!(!parts.eq(&other_parts));

		let s2 = vec![43, 1, 200];
		let other_parts = SliceParts::from_slice(s2.as_slice());
		assert_eq!(parts.partial_cmp(&other_parts), Some(Ordering::Less));
		assert!(!parts.eq(&other_parts));
		let s2 = vec![42, 84, 126];
		let other_parts = SliceParts::from_slice(s2.as_slice());
		assert_eq!(parts.partial_cmp(&other_parts), Some(Ordering::Equal));
		assert!(parts.eq(&other_parts));
	}

	#[test]
	fn test_kv_pair() {
		let k = vec![1, 2, 3, 4];
		let v = vec![5, 6, 7, 8, 9, 0];

		let kv = KVPair::new(&k, &v);
		assert_eq!(kv.key(), &k[..]);
		assert_eq!(kv.value(), &v[..]);
		assert_eq!(kv.size(), 10);

		let kv = KVPair::from_slice_parts(SliceParts::from_slice(&k), SliceParts::from_slice(&v));
		assert_eq!(kv.key(), &k[..]);
		assert_eq!(kv.value(), &v[..]);
		assert_eq!(kv.size(), 10);
	}

	#[test]
	fn test_bucket_data() {
		let name = b"Hello Bucket!";
		let mut meta = BucketMeta {
			root_page: 3,
			next_int: 24_985_738_796,
		};

		let b = BucketData::new(name, meta.as_ref());
		assert_eq!(b.name(), name);
		assert_eq!(b.meta().root_page, meta.root_page);
		assert_eq!(b.meta().next_int, meta.next_int);
		assert_eq!(b.size(), 13 + std::mem::size_of_val(&meta) as u64);

		meta.next_int += 1;
		assert_eq!(b.meta().next_int, meta.next_int);

		let b = BucketData::from_slice_parts(
			SliceParts::from_slice(name),
			SliceParts::from_slice(meta.as_ref()),
		);
		assert_eq!(b.name(), name);
		assert_eq!(b.meta().root_page, meta.root_page);
		assert_eq!(b.meta().next_int, meta.next_int);
		assert_eq!(b.size(), 13 + std::mem::size_of_val(&meta) as u64);

		meta.next_int += 1;
		assert_eq!(b.meta().next_int, meta.next_int);

		let b = BucketData::from_meta(SliceParts::from_slice(name), &meta);
		assert_eq!(b.name(), name);
		assert_eq!(b.meta().root_page, meta.root_page);
		assert_eq!(b.meta().next_int, meta.next_int);
		assert_eq!(b.size(), 13 + std::mem::size_of_val(&meta) as u64);

		meta.next_int += 1;
		assert_eq!(b.meta().next_int, meta.next_int);
	}

	#[test]
	fn test_data() {
		let k = vec![1, 2, 3, 4, 5, 6, 7, 8];
		let v = vec![11, 22, 33, 44, 55, 66, 77, 88];

		let data: Data = Data::KeyValue(KVPair::new(&k, &v));

		assert_eq!(data.node_type(), Node::TYPE_DATA);
		assert_eq!(data.key_parts(), SliceParts::from_slice(&k));
		assert_eq!(data.key(), &k[..]);
		assert_eq!(data.value(), &v[..]);
		assert_eq!(data.size(), 16);

		let meta = BucketMeta {
			root_page: 456,
			next_int: 8_888_888,
		};
		let data: Data = Data::Bucket(BucketData::new(&k, meta.as_ref()));

		assert_eq!(data.node_type(), Node::TYPE_BUCKET);
		assert_eq!(data.key_parts(), SliceParts::from_slice(&k));
		assert_eq!(data.key(), &k[..]);
		assert_eq!(data.value(), meta.as_ref());
		assert_eq!(data.size(), 8 + std::mem::size_of_val(&meta) as u64);
	}
}
