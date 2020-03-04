use std::fmt;
use std::hash::{Hash, Hasher};
use std::cmp::{Ord, Ordering};

use crate::bucket::{BucketMeta, Bucket};
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
			Data::Bucket(b) => Node::TYPE_BUCKET,
			Data::KeyValue(kv) => Node::TYPE_DATA,
		}		
	}

	pub (crate) fn key_parts(&self) -> SliceParts {
		match self {
			Data::Bucket(b) => b.0[0],
			Data::KeyValue(kv) => kv.0[0],
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
			Data::Bucket(b) => b.0[1].slice(),
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
pub struct BucketData([SliceParts; 2]);

impl BucketData {
	pub (crate) fn new(name: &[u8], meta: &[u8]) -> Data {
		let b = BucketData([
			SliceParts::from_slice(name),
			SliceParts::from_slice(meta),
		]);
		// println!("NEW BUCKET: {:?}", std::str::from_utf8(b.name()));
		Data::Bucket(b)
	}

	pub (crate) fn from_slice_parts(key: SliceParts, meta: SliceParts) -> Data {
		Data::Bucket(BucketData([key, meta]))
	}

	pub (crate) fn from_bucket(name: SliceParts, b: &Bucket) -> Data {
		Data::Bucket(BucketData([
			name,
			SliceParts{
				ptr: &b.meta as *const BucketMeta as *const u8,
				len: std::mem::size_of::<BucketMeta>(),
			},
		]))
	}

	pub fn name(&self) -> &[u8] {
		unsafe{ std::slice::from_raw_parts(self.0[0].ptr, self.0[0].len) }
	}

	pub (crate) fn meta(&self) -> BucketMeta {
		unsafe{ *(self.0[1].ptr as *const BucketMeta) }
	}

	pub fn size(&self) -> usize {
		self.0[0].size() + self.0[1].size()
	}
}

#[derive(Clone, Debug)]
pub struct KVPair([SliceParts; 2]);

impl KVPair {
	fn new(key: &[u8], value: &[u8]) -> Data {
		Data::KeyValue(KVPair([
			SliceParts::from_slice(key),
			SliceParts::from_slice(value),
		]))
	}

	pub (crate) fn from_slice_parts(key: SliceParts, value: SliceParts) -> Data {
		Data::KeyValue(KVPair([key, value]))
	}

	pub fn key(&self) -> &[u8] {
		self.0[0].slice()
	}

	pub fn value(&self) -> &[u8] {
		self.0[1].slice()
	}

	pub fn size(&self) -> usize {
		self.0[0].size() + self.0[1].size()
	}
}

#[derive(Clone, Copy)]
pub (crate) struct SliceParts {
	pub (crate) ptr: *const u8,
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
