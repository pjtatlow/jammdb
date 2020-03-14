use std::mem::size_of;
use std::collections::{BTreeSet, BTreeMap};

use crate::page::{PageID, Page};

#[derive(Clone)]
pub (crate) struct Freelist {
	free_pages: BTreeSet<PageID>,
	pending_pages: BTreeMap<u64, Vec<PageID>>,
}

const HEADER_SIZE: usize = size_of::<Page>();
const PAGE_ID_SIZE: usize = size_of::<PageID>();

impl Freelist {
	pub (crate) fn new() -> Freelist {
		Freelist{
			free_pages: BTreeSet::new(),
			pending_pages: BTreeMap::new(),
		}
	}

	pub (crate) fn init(&mut self, free_pages: &[PageID]) {
		free_pages.iter().for_each(|id| { self.free_pages.insert(*id); });
	}

	// adds the page to the transaction's set of free pages
	pub (crate) fn free(&mut self, tx_id: u64, page_id: PageID) {
		let pages = self.pending_pages.entry(tx_id).or_insert_with(Vec::new);
		pages.push(page_id);
	}

	// frees all pages from old transactions that have lower ids than the given tx_id
	pub (crate) fn release(&mut self, tx_id: u64) {
		let pending_ids: Vec<u64> = self.pending_pages.keys().cloned().collect();
		for other_tx_id in pending_ids {
			if other_tx_id < tx_id {
				let pages = self.pending_pages.remove(&other_tx_id).unwrap();
				pages.into_iter().for_each(|p| { self.free_pages.insert(p); } );
			} else {
				break;
			}
		}
	}

	pub (crate) fn allocate(&mut self, num_pages: usize) -> Option<PageID> {
		if self.free_pages.is_empty() {
			return None;
		}
		let mut start: PageID = 0;
		let mut prev: PageID = 0;
		let mut found: PageID = 0;

		for id in self.free_pages.iter().cloned() {
			debug_assert!(id > 1, "invalid pageID in freelist");

			if prev == 0 || id - prev != 1 {
				start = id;
			}

			let block_size = id - start + 1;
			if block_size == num_pages {
				found = start;
				break;
			}

			prev = id;
		}

		if found > 0 {
			for id in found..found+num_pages {
				self.free_pages.remove(&id);
			}
			return Some(found);
		}

		None
	}

	pub (crate) fn pages(&self) -> Vec<PageID> {
		let mut page_ids: Vec<PageID> = self.free_pages.iter().cloned().collect();
		for (_, pages) in self.pending_pages.iter() {
			let mut pages = pages.to_vec();
			page_ids.append(&mut pages);
		}
		page_ids.sort_unstable();
		page_ids
	}

	pub (crate) fn size(&self) -> usize {
		let count = self.pages().len();
		HEADER_SIZE + (PAGE_ID_SIZE * count)
	}
}

#[cfg(test)]
mod tests {
    use super::*;

	fn freelist_from_vec(v: Vec<PageID>) -> Freelist {
		let mut freelist = Freelist{
			free_pages: v.iter().cloned().collect(),
			pending_pages: BTreeMap::new(),
		};
		freelist.init(v.as_slice());
		freelist
	}

    #[test]
    fn test_allocate() {
		let mut freelist = freelist_from_vec(vec![2,4,6,8,9,10]);
		
		assert_eq!(freelist.allocate(4), None);
		assert_eq!(freelist.allocate(1), Some(2));
		assert_eq!(freelist.free_pages.iter().cloned().collect::<Vec<usize>>(), vec![4,6,8,9,10]);
		assert_eq!(freelist.allocate(1), Some(4));
		assert_eq!(freelist.free_pages.iter().cloned().collect::<Vec<usize>>(), vec![6,8,9,10]);
		assert_eq!(freelist.allocate(3), Some(8));
		assert_eq!(freelist.free_pages.iter().cloned().collect::<Vec<usize>>(), vec![6]);
		assert_eq!(freelist.allocate(1), Some(6));
		assert_eq!(freelist.free_pages.iter().cloned().collect::<Vec<usize>>(), vec![]);
		assert_eq!(freelist.allocate(1), None);
    }

	#[test]
	fn test_free() {
		let mut freelist = Freelist::new();

		freelist.free(1, 5);
		assert_eq!(freelist.pending_pages.len(), 1);
		assert_eq!(freelist.pending_pages.get(&1), Some(&vec![5]));
		
		freelist.free(1, 4);
		assert_eq!(freelist.pending_pages.len(), 1);
		assert_eq!(freelist.pending_pages.get(&1), Some(&vec![5, 4]));
		
		freelist.free(1, 3);
		assert_eq!(freelist.pending_pages.len(), 1);
		assert_eq!(freelist.pending_pages.get(&1), Some(&vec![5, 4, 3]));
		
		freelist.free(2, 7);
		assert_eq!(freelist.pending_pages.len(), 2);
		assert_eq!(freelist.pending_pages.get(&1), Some(&vec![5, 4, 3]));
		assert_eq!(freelist.pending_pages.get(&2), Some(&vec![7]));
		
		assert_eq!(freelist.free_pages, BTreeSet::new());
	}


	#[test]
	fn test_pages() {
		let mut freelist = freelist_from_vec(vec![1,2,3,4,5]);

		assert_eq!(freelist.pages(), vec![1,2,3,4,5]);
		
		freelist.free(2, 9);
		freelist.free(2, 10);
		freelist.free(2, 11);
		assert_eq!(freelist.pages(), vec![1,2,3,4,5,9,10,11]);

		freelist.free(1, 6);
		freelist.free(1, 7);
		freelist.free(1, 8);
		assert_eq!(freelist.pages(), vec![1,2,3,4,5,6,7,8,9,10,11]);
	}

	#[test]
	fn test_release() {
		let mut freelist = Freelist::new();

		freelist.free(1, 5);
		freelist.free(1, 10);
		freelist.free(1, 7);

		assert_eq!(freelist.free_pages.len(), 0);
		assert_eq!(freelist.pending_pages.len(), 1);
		
		freelist.release(1);
		
		assert_eq!(freelist.free_pages.len(), 0);
		assert_eq!(freelist.pending_pages.len(), 1);

		freelist.release(2);
		
		assert_eq!(freelist.free_pages.len(), 3);
		assert_eq!(freelist.pending_pages.len(), 0);
		assert_eq!(freelist.pages(), vec![5,7,10]);
	}

	#[test]
	fn test_size() {
		let freelist = freelist_from_vec(vec![1,2,3]);
		assert_eq!(freelist.size(), HEADER_SIZE + (PAGE_ID_SIZE * 3));
	}
}