use std::fs::File;
use std::mem::size_of;
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::ops::Deref;

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
	bucket: Ptr<Bucket>,
	// pub (crate) key: SliceParts,
	pub (crate) parent: Option<PageNodeID>,
	pub (crate) children: Vec<NodeID>,
	pub (crate) data: NodeData,
	unbalanced: bool,
	spilled: bool,
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

	fn element_size(&self) -> usize {
		match self {
			NodeData::Branches(_) => BRANCH_SIZE,
			NodeData::Leaves(_) => LEAF_SIZE,
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

	fn split_up(&mut self, indexes: Vec<usize>) -> Vec<NodeData> {
		let mut data = Vec::<NodeData>::new();
		let iter = indexes.iter().rev().map(|i| self.split_at(*i));
		data
	}

	fn page_type(&self) -> PageType {
		match self {
			NodeData::Branches(_) => Page::TYPE_BRANCH,
			NodeData::Leaves(_) => Page::TYPE_LEAF,
		}			
	}

}

pub (crate) struct Branch {
	key: SliceParts,
	pub(crate) page: PageID,
}

impl Branch {
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
			bucket: b,
			parent: None,
			children: Vec::new(),
			data,
			unbalanced: false,
			spilled: false,
		}		
	}

	pub (crate) fn with_data(id: NodeID, data: NodeData, b: Ptr<Bucket>) -> Node {
		Node{
			id,
			page_id: 0,
			bucket: b,
			parent: None,
			children: Vec::new(),
			data,
			unbalanced: false,
			spilled: false,
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
				// println!("PAGE_TYPE: {}", p.page_type);
				panic!("INVALID PAGE TYPE FOR FROM_PAGE")
			},
		};
		// println!("DATA: {:?} PAGE_ID: {}", data.len(), p.id);
		Node{
			id,
			page_id: p.id,
			bucket: b,
			parent: None,
			children: Vec::new(),
			data,
			unbalanced: false,
			spilled: false,
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

	pub (crate) fn insert_branch(&mut self, id: NodeID, key: SliceParts) {
		match &mut self.data {
			NodeData::Branches(branches) => {
				let index = match branches.binary_search_by_key(&key.slice(), |b| &b.key()) {
					Ok(i) => panic!("BRANCH ALREADY EXISTS"),
					Err(i) => i,
				};
				branches.insert(index, Branch{
					key,
					page: 0,
				});
				self.children.push(id);
				let mut b = Ptr::new(&self.bucket);
				self.children.sort_by_cached_key(|id| b.node(PageNodeID::Node(*id)).data.key_parts())
			},
			NodeData::Leaves(_) => panic!("CANNOT INSERT BRANCH INTO A LEAF NODE"),
		}
	}

	fn size(&self) -> usize {
		HEADER_SIZE + self.data.size()
	}
	
	pub (crate) fn write(&mut self, file: &mut File) -> Result<PageID> {
		for node_id in self.children.iter() {
			let node = self.bucket.node(PageNodeID::Node(*node_id));
			let page_id = node.write(file)?;
			if let NodeData::Branches(branches) = &mut self.data {
				let key = node.data.key_parts();
				let key = key.slice();
				match branches.binary_search_by_key(&key, |b| b.key()) {
					Ok(i) => branches[i].page = page_id,
					Err(_) => panic!("NOOOO"),
				}
			}
		};
		let size = self.size();
		// TODO release old page
		let (page_id, num_pages) = self.bucket.tx.allocate(size);
		self.page_id = page_id;
		let mut buf: Vec<u8> = vec![0; size];
		let page = unsafe {&mut *(&mut buf[0] as *mut u8 as *mut Page)};
		page.write_node(self, num_pages)?;
		let offset = (page_id as u64) * (self.bucket.tx.meta.pagesize as u64);
		// println!("WRITING PAGE: {:?} at {}", page, offset);
		file.write_all_at(buf.as_slice(), offset)?;
		Ok(self.page_id)
	}

	pub (crate) fn merge(&mut self) {
		
	}

	pub (crate) fn split(&mut self) {
		let mut i = 0;
		let mut len = self.children.len();
		while i < len {
			let child = self.bucket.node(PageNodeID::Node(self.children[i]));
			child.split();
			i += 1;
			// len can change from the child splitting
			len = self.children.len();
		}
		if self.data.len() <= (MIN_KEYS_PER_NODE * 2) || self.size() < self.bucket.tx.db.pagesize {
			return;
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
						current_size = HEADER_SIZE;
						count = 0;
					}
				}
			},
			NodeData::Leaves(leaves) => {
				for (i, l) in leaves.iter().enumerate() {
					count += 1;
					let size = BRANCH_SIZE + l.size();
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
		if split_indexes.len() == 0 {
			return;
		}
		
		// create a new root node

		if self.parent.is_none() {
			let parent = self.bucket.new_node(NodeData::Branches(Vec::new()));
			parent.insert_branch(self.id, self.data.key_parts());
			self.parent = Some(PageNodeID::Node(parent.id));
			self.bucket.root = PageNodeID::Node(parent.id);
		}

		let new_data: Vec<NodeData> = split_indexes.iter().rev().map(|i| self.data.split_at(*i)).collect();
		
		let mut b = Ptr::new(self.bucket.deref());

		for data in new_data {
			let id: PageID;
			let key = data.key_parts();
			{
				let n = b.new_node(data);
				n.parent = self.parent;
				id = n.id;
			}
			let parent = b.node(self.parent.unwrap());
			parent.insert_branch(id, key);
		}


	}
}