use std::mem::size_of;

use sha3::{Digest, Sha3_256};

use crate::bucket::BucketMeta;
use crate::page::PageID;

const META_SIZE: usize = size_of::<Meta>();

#[repr(C)]
#[derive(Debug, Clone)]
pub(crate) struct Meta {
	pub(crate) meta_page: u8,
	pub(crate) magic: u32,
	pub(crate) version: u32,
	pub(crate) pagesize: u32,
	pub(crate) root: BucketMeta,
	pub(crate) num_pages: PageID,
	pub(crate) freelist_page: PageID,
	pub(crate) tx_id: u64,
	pub(crate) hash: [u8; 32],
}

impl Meta {
	pub(crate) fn valid(&self) -> bool {
		self.hash == self.hash_self()
	}

	pub(crate) fn hash_self(&self) -> [u8; 32] {
		let mut hash_result: [u8; 32] = [0; 32];
		let mut hasher = Sha3_256::new();
		hasher.input(self.bytes());
		let hash = hasher.result();
		hash_result.copy_from_slice(&hash[..]);
		hash_result
	}

	fn bytes(&self) -> &[u8] {
		let ptr = self as *const Meta as *const u8;
		unsafe { std::slice::from_raw_parts(ptr, META_SIZE - 32) }
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_meta() {
		let mut meta = Meta {
			meta_page: 1,
			magic: 1_234_567_890,
			version: 987_654_321,
			pagesize: 4096,
			root: BucketMeta {
				root_page: 2,
				sequence: 2020,
			},
			num_pages: 13,
			freelist_page: 3,
			tx_id: 8,
			hash: [0; 32],
		};

		assert!(!meta.valid());
		meta.hash = meta.hash_self();
		assert!(meta.valid());
		assert_eq!(meta.hash, meta.hash_self());
		// modify the last property before the hash
		// to change the hash
		meta.tx_id = 88;
		assert_ne!(meta.hash, meta.hash_self());
		// reset hash and make sure it is still valid
		meta.hash = meta.hash_self();
		assert!(meta.valid());
		assert_eq!(meta.hash, meta.hash_self());
	}
}
