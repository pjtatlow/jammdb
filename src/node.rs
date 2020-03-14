use std::fs::File;
use std::mem::size_of;
use std::os::unix::fs::FileExt;

use crate::page::{Page, PageID, PageType, LeafElement, BranchElement};
use crate::ptr::Ptr;
use crate::bucket::Bucket;
use crate::data::{Data, SliceParts};
use crate::cursor::PageNodeID;
use crate::errors::Result;

pub (crate) type NodeID = usize;

const HEADER_SIZE: usize = size_of::<Page>();
const LEAF_SIZE: usize = size_of::<LeafElement>();
const BRANCH_SIZE: usize = size_of::<BranchElement>();
const MIN_KEYS_PER_NODE: usize = 2;
const FILL_PERCENT: f32 = 0.5;

pub (crate) struct Node {
	pub (crate) id: NodeID,
	pub (crate) page_id: PageID,
	pub (crate) num_pages: usize,
	bucket: Ptr<Bucket>,
	// pub (crate) key: SliceParts,
	pub (crate) children: Vec<NodeID>,
	pub (crate) data: NodeData,
}

pub (crate) enum NodeData {
	Branches(Vec<Branch>),
	Leaves(Vec<Data>),
}

impl NodeData {
	pub (crate) fn len(&self) -> usize {
		match self {
			NodeData::Branches(b) => b.len(),
			NodeData::Leaves(l) => l.len(),
		}
	}

	fn size(&self) -> usize {
		match self {
			NodeData::Branches(b) => b.iter().fold(BRANCH_SIZE * b.len(), |acc, b| acc + b.key_size()) ,
			NodeData::Leaves(l) => l.iter().fold(LEAF_SIZE * l.len(), |acc, l| acc + l.size()),
		}
	}

	pub (crate) fn key_parts(&self) -> SliceParts {
		debug_assert!(self.len() > 0, "Cannot get key parts of empty data");
		match self {
			NodeData::Branches(b) => b.first().map(|b| b.key),
			NodeData::Leaves(l) => l.first().map(|l| l.key_parts()),
		}.unwrap()
	}

	fn split_at(&mut self, index: usize) -> NodeData {
		match self {
			NodeData::Branches(b) => NodeData::Branches(b.split_off(index)),
			NodeData::Leaves(l) => NodeData::Leaves(l.split_off(index)),
		}		
	}

}

pub (crate) struct Branch {
	key: SliceParts,
	pub(crate) page: PageID,
}

impl Branch {
	pub (crate) fn from_node(node: &Node) -> Branch {
		Branch{
			key: node.data.key_parts(),
			page: node.page_id,
		}
	}

	pub (crate) fn key(&self) -> &[u8] {
		self.key.slice()
	}

	pub (crate) fn key_size(&self) -> usize {
		self.key.size()
	}
}

// Change to DataType
pub (crate) type NodeType = u8;


impl Node {
	pub (crate) const TYPE_DATA: NodeType = 0x00;
	pub (crate) const TYPE_BUCKET: NodeType = 0x01;

	pub (crate) fn new(id: NodeID, t: PageType, b: Ptr<Bucket>) -> Node {
		let data: NodeData = match t {
			Page::TYPE_BRANCH => {
				NodeData::Branches(Vec::new())
			},
			Page::TYPE_LEAF => {
				NodeData::Leaves(Vec::new())
			},
			_ => panic!("INVALID PAGE TYPE FOR NEW NODE"),
		};
		Node{
			id,
			page_id: 0,
			num_pages: 0,
			bucket: b,
			children: Vec::new(),
			data,
		}		
	}

	pub (crate) fn with_data(id: NodeID, data: NodeData, b: Ptr<Bucket>) -> Node {
		Node{
			id,
			page_id: 0,
			num_pages: 0,
			bucket: b,
			children: Vec::new(),
			data,
		}
	}

	pub (crate) fn from_page(id: NodeID, b: Ptr<Bucket>, p: &Page) -> Node {
		let data: NodeData = match p.page_type {
			Page::TYPE_BRANCH => {
				let mut data = Vec::with_capacity(p.count as usize);
				for branch in p.branch_elements() {
					data.push(Branch{
						key: SliceParts::from_slice(branch.key()),
						page: branch.page,
					});
				}
				NodeData::Branches(data)
			},
			Page::TYPE_LEAF => {
				let mut data = Vec::with_capacity(p.count as usize);
				for leaf in p.leaf_elements() {
					data.push(Data::from_leaf(leaf));
				}
				NodeData::Leaves(data)
			},
			_ => {
				panic!("INVALID PAGE TYPE FOR FROM_PAGE")
			},
		};
		Node{
			id,
			page_id: p.id,
			num_pages: p.overflow + 1,
			bucket: b,
			children: Vec::new(),
			data,
		}
	}

	pub (crate) fn leaf(&self) -> bool {
		match &self.data {
			NodeData::Branches(_) => false,
			NodeData::Leaves(_) => true,
		}
	}

	pub (crate) fn insert_data(&mut self, data: Data) {
		match &mut self.data {
			NodeData::Branches(_) => panic!("CANNOT INSERT DATA INTO A BRANCH NODE"),
			NodeData::Leaves(leaves) => {
				match leaves.binary_search_by_key(&data.key(), |d| &d.key()) {
					Ok(i) => leaves[i] = data,
					Err(i) => leaves.insert(i, data),
				};
			},
		}
	}

	pub (crate) fn insert_child(&mut self, id: NodeID, key: SliceParts) {
		match &mut self.data {
			NodeData::Branches(branches) => {
				debug_assert!(!self.children.contains(&id));
				debug_assert!(branches.binary_search_by_key(&key.slice(), |b| &b.key()).is_ok());
				self.children.push(id);
			},
			NodeData::Leaves(_) => panic!("CANNOT INSERT BRANCH INTO A LEAF NODE"),
		}
	}

	fn size(&self) -> usize {
		HEADER_SIZE + self.data.size()
	}
	
	pub (crate) fn write(&mut self, file: &mut File) -> Result<()> {
		let size = self.size();
		let mut buf: Vec<u8> = vec![0; size];
		
		#[allow(clippy::cast_ptr_alignment)]
		let page = unsafe {&mut *(&mut buf[0] as *mut u8 as *mut Page)};
		page.write_node(self, self.num_pages)?;
		let offset = (self.page_id as u64) * (self.bucket.tx.meta.pagesize as u64);
		// println!("WRITING PAGE: {:?} at {}", page, offset);
		file.write_all_at(buf.as_slice(), offset)?;
		Ok(())
	}

	pub (crate) fn merge(&mut self) {
		
	}

	fn allocate(&mut self) {
		if self.page_id != 0 {
			self.bucket.tx.free(self.page_id, self.num_pages);
		}
		let size = self.size();
		let (page_id, num_pages) = self.bucket.tx.allocate(size);
		self.page_id = page_id;
		self.num_pages = num_pages;
	}

	pub (crate) fn split(&mut self) -> Option<Vec<Branch>> {
		let mut last_branch_index = 0;
		// sort children so we iterate over them in order
		let mut b = Ptr::new(&self.bucket);
		self.children.sort_by_cached_key(|id| b.node(PageNodeID::Node(*id)).data.key_parts());
		for child in self.children.iter() {
			let child = self.bucket.node(PageNodeID::Node(*child));
			let new_branches = child.split();
			if let NodeData::Branches(branches) = &mut self.data {
				match &branches[last_branch_index..].binary_search_by_key(&child.data.key_parts().slice(), |b| b.key()) {
					Ok(i) => last_branch_index = *i,
					_ => panic!("THIS IS VERY VERY BAD"),
				}
				branches[last_branch_index] = Branch::from_node(&child);
				if let Some(mut new_branches) = new_branches {
					let mut right_side = branches.split_off(last_branch_index + 1);
					branches.append(&mut new_branches);
					branches.append(&mut right_side);
				}
			}
		}
		if self.data.len() <= (MIN_KEYS_PER_NODE * 2) || self.size() < self.bucket.tx.db.pagesize {
			self.allocate();
			return None;
		}
		let threshold = ((self.bucket.tx.db.pagesize as f32) * FILL_PERCENT) as usize;
		let mut split_indexes = Vec::<usize>::new();
		let mut current_size = HEADER_SIZE;
		let mut count = 0;
		match &self.data {
			NodeData::Branches(b) => {
				for (i, b) in b.iter().enumerate() {
					count += 1;
					let size = BRANCH_SIZE + b.key_size();
					let new_size = current_size + size;
					if count >= MIN_KEYS_PER_NODE && new_size > threshold {
						split_indexes.push(i);
						current_size = HEADER_SIZE + size;
						count = 0;
					} else {
						current_size = new_size;
					}
				}
			},
			NodeData::Leaves(leaves) => {
				for (i, l) in leaves.iter().enumerate() {
					count += 1;
					let size = LEAF_SIZE + l.size();
					let new_size = current_size + size;
					if count >= MIN_KEYS_PER_NODE && new_size > threshold {
						split_indexes.push(i);
						current_size = HEADER_SIZE + size;
						count = 0;
					} else {
						current_size = new_size;
					}
				}
			},
		};
		// for some reason we didn't find a place to split
		if split_indexes.is_empty() {
			self.allocate();
			return None;
		}

		let new_data: Vec<NodeData> = split_indexes.iter()
			// split from the end so we only break off small chunks at a time
			.rev()
			// split the data
			.map(|i| self.data.split_at(*i))
			.collect();
		
		
		self.allocate();

		Some(new_data.into_iter().rev().map(|data| {
			let n = self.bucket.new_node(data);
			n.allocate();
			Branch::from_node(n)
		}).collect())
	}
}