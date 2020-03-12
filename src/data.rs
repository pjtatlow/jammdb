use std::fmt;
use std::hash::{Hash, Hasher};
use std::cmp::{Ord, Ordering};

use crate::bucket::BucketMeta;
use crate::page::LeafElement;
use crate::node::{Node, NodeType};

#[derive(Clone, Debug)]
pub enum Data {
	Bucket(BucketData),
	KeyValue(KVPair),
}

impl Data {
	pub (crate) fn from_leaf(l: &LeafElement) -> Data {
		match l.node_type {
			Node::TYPE_DATA => KVPair::new(l.key(), l.value()),
			Node::TYPE_BUCKET => BucketData::new(l.key(), l.value()),
			_ => panic!("INVALID NODE TYPE"),
		}
	}

	pub (crate) fn node_type(&self) -> NodeType {
		match self {
			Data::Bucket(_) => Node::TYPE_BUCKET,
			Data::KeyValue(_) => Node::TYPE_DATA,
		}		
	}

	pub (crate) fn key_parts(&self) -> SliceParts {
		match self {
			Data::Bucket(b) => b.name,
			Data::KeyValue(kv) => kv.key,
		}
	}

	pub (crate) fn key(&self) -> &[u8] {
		match self {
			Data::Bucket(b) => b.name(),
			Data::KeyValue(kv) => kv.key(),
		}
	}

	pub (crate) fn value(&self) -> &[u8] {
		match self {
			Data::Bucket(b) => b.meta.slice(),
			Data::KeyValue(kv) => kv.value(),
		}
	}

	pub (crate) fn size(&self) -> usize {
		match self {
			Data::Bucket(b) => b.size(),
			Data::KeyValue(kv) => kv.size(),
		}
	}	
}

#[derive(Clone, Debug)]
pub struct BucketData {
	name: SliceParts,
	meta: SliceParts,
}

impl BucketData {
	pub (crate) fn new(name: &[u8], meta: &[u8]) -> Data {
		let b = BucketData{
			name: SliceParts::from_slice(name),
			meta: SliceParts::from_slice(meta),
		};
		Data::Bucket(b)
	}

	pub (crate) fn from_slice_parts(name: SliceParts, meta: SliceParts) -> Data {
		Data::Bucket(BucketData{name, meta})
	}

	pub (crate) fn from_meta(name: SliceParts, meta: &BucketMeta) -> Data {
		Data::Bucket(BucketData{
			name,
			meta: SliceParts::from_slice(meta.as_ref()),
		})
	}

	pub fn name(&self) -> &[u8] {
		self.name.slice()
	}

	pub (crate) fn meta(&self) -> BucketMeta {
		unsafe{ *(self.meta.ptr as *const BucketMeta) }
	}

	pub fn size(&self) -> usize {
		self.name.size() + self.meta.size()
	}
}

#[derive(Clone, Debug)]
pub struct KVPair{
	key: SliceParts,
	value: SliceParts,
}

impl KVPair {
	fn new(key: &[u8], value: &[u8]) -> Data {
		Data::KeyValue(KVPair{
			key: SliceParts::from_slice(key),
			value: SliceParts::from_slice(value),
		})
	}

	pub (crate) fn from_slice_parts(key: SliceParts, value: SliceParts) -> Data {
		Data::KeyValue(KVPair{key, value})
	}

	pub fn key(&self) -> &[u8] {
		self.key.slice()
	}

	pub fn value(&self) -> &[u8] {
		self.value.slice()
	}

	pub fn size(&self) -> usize {
		self.key.size() + self.value.size()
	}
}

#[derive(Clone, Copy)]
pub (crate) struct SliceParts {
	ptr: *const u8,
	len: usize,
}

impl SliceParts {
	pub (crate) fn from_slice(s: &[u8]) -> SliceParts {
		let ptr;
		let len = s.len();
		if len > 0 {
			ptr = &s[0] as *const u8;
		} else {
			ptr = 0 as *const u8;
		}
		SliceParts{
			ptr,
			len,
		}		
	}


	pub (crate) fn slice(&self) -> &[u8] {
		unsafe{ std::slice::from_raw_parts(self.ptr, self.len) }
	}

	pub (crate) fn size(&self) -> usize {
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
		let s2 = vec![41,85,142];
		let other_parts = SliceParts::from_slice(s2.as_slice());
		assert_eq!(parts.partial_cmp(&other_parts), Some(Ordering::Greater));
		assert!(parts.eq(&other_parts) == false);
		
		let s2 = vec![43,1,200];
		let other_parts = SliceParts::from_slice(s2.as_slice());
		assert_eq!(parts.partial_cmp(&other_parts), Some(Ordering::Less));
		assert!(parts.eq(&other_parts) == false);
		
		let s2 = vec![42, 84, 126];
		let other_parts = SliceParts::from_slice(s2.as_slice());
		assert_eq!(parts.partial_cmp(&other_parts), Some(Ordering::Equal));
		assert!(parts.eq(&other_parts) == true);
	}

	#[test]
	fn test_kv_pair() {
		let k = vec![1,2,3,4];
		let v = vec![5,6,7,8,9,0];

		match KVPair::new(&k, &v) {
			Data::KeyValue(kv) => {
				assert_eq!(kv.key(), &k[..]);
				assert_eq!(kv.value(), &v[..]);
				assert_eq!(kv.size(), 10);
			}, 
			_ => panic!("Expected Data::KeyValue"),
		}

		match KVPair::from_slice_parts(SliceParts::from_slice(&k), SliceParts::from_slice(&v)) {
			Data::KeyValue(kv) => {
				assert_eq!(kv.key(), &k[..]);
				assert_eq!(kv.value(), &v[..]);
				assert_eq!(kv.size(), 10);
			}, 
			_ => panic!("Expected Data::KeyValue"),
		}

	}

	#[test]
	fn test_bucket_data() {
		let name = "Hello Bucket!".as_bytes();
		let mut meta = BucketMeta{root_page: 3, sequence: 24985738796};

		match BucketData::new(name, meta.as_ref()) {
			Data::Bucket(b) => {
				assert_eq!(b.name(), name);
				assert_eq!(b.meta().root_page, meta.root_page);
				assert_eq!(b.meta().sequence, meta.sequence);
				assert_eq!(b.size(), 13 + std::mem::size_of_val(&meta));


				meta.sequence += 1;
				assert_eq!(b.meta().sequence, meta.sequence);
			},
			_ => panic!("Expected Data::Bucket"),
		}

		match BucketData::from_slice_parts(SliceParts::from_slice(name), SliceParts::from_slice(meta.as_ref())) {
			Data::Bucket(b) => {
				assert_eq!(b.name(), name);
				assert_eq!(b.meta().root_page, meta.root_page);
				assert_eq!(b.meta().sequence, meta.sequence);
				assert_eq!(b.size(), 13 + std::mem::size_of_val(&meta));

				meta.sequence += 1;
				assert_eq!(b.meta().sequence, meta.sequence);
			},
			_ => panic!("Expected Data::Bucket"),
		}

		match BucketData::from_meta(SliceParts::from_slice(name), &meta) {
			Data::Bucket(b) => {
				assert_eq!(b.name(), name);
				assert_eq!(b.meta().root_page, meta.root_page);
				assert_eq!(b.meta().sequence, meta.sequence);
				assert_eq!(b.size(), 13 + std::mem::size_of_val(&meta));

				meta.sequence += 1;
				assert_eq!(b.meta().sequence, meta.sequence);
			},
			_ => panic!("Expected Data::Bucket"),
		}
	}

	#[test]
	fn test_data() {
		let k = vec![1,2,3,4,5,6,7,8];
		let v = vec![11,22,33,44,55,66,77,88];

		let data: Data = KVPair::new(&k, &v);

		assert_eq!(data.node_type(), Node::TYPE_DATA);
		assert_eq!(data.key_parts(), SliceParts::from_slice(&k));
		assert_eq!(data.key(), &k[..]);
		assert_eq!(data.value(), &v[..]);
		assert_eq!(data.size(), 16);

		let meta = BucketMeta{root_page: 456, sequence: 8888888};
		let data: Data = BucketData::new(&k, meta.as_ref());

		assert_eq!(data.node_type(), Node::TYPE_BUCKET);
		assert_eq!(data.key_parts(), SliceParts::from_slice(&k));
		assert_eq!(data.key(), &k[..]);
		assert_eq!(data.value(), meta.as_ref());
		assert_eq!(data.size(), 8 + std::mem::size_of_val(&meta));
	}

}