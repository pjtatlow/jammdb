use crate::ptr::Ptr;
use crate::bucket::{Bucket};
use crate::page::{Page, PageID};
use crate::node::{Node, NodeID, NodeData};
use crate::data::Data;

#[derive(Clone, Copy)]
pub (crate) enum PageNodeID {
	Page(PageID),
	Node(NodeID),
}

pub (crate) enum PageNode {
	Page(Ptr<Page>),
	Node(Ptr<Node>),
}

impl PageNode {
	fn leaf(&self) -> bool {
		match self {
			PageNode::Page(p) => p.page_type == Page::TYPE_LEAF,
			PageNode::Node(n) => n.leaf(),
		}
	}

	fn len(&self) -> usize {
		match self {
			PageNode::Page(p) => p.count as usize,
			PageNode::Node(n) => n.data.len(),
		}
	}

	fn index_page(&self, index: usize) -> PageID {
		match self {
			PageNode::Page(p) => {
				if index >= p.count as usize {
					return 0;
				}
				match p.page_type {
					Page::TYPE_BRANCH => p.branch_elements()[index].page,
					_ => panic!("INVALID PAGE TYPE FOR INDEX_PAGE"),
				}
			},
			PageNode::Node(n) => {
				if index >= n.data.len() {
					return 0;
				}				
				match &n.data {
					NodeData::Branches(b) => b[index].page,
					_ => panic!("INVALID NODE TYPE FOR INDEX_PAGE"),
				}
			},
		}
	}

	fn index(&self, key: &[u8]) -> (usize, bool) {
		let result = match self {
			PageNode::Page(p) => {
				match p.page_type {
					Page::TYPE_LEAF => p.leaf_elements().binary_search_by_key(&key, |e| e.key()),
					Page::TYPE_BRANCH => p.branch_elements().binary_search_by_key(&key, |e| e.key()),
					_ => panic!("INVALID PAGE TYPE FOR INDEX: {:?}", p.page_type),
				}
			},
			PageNode::Node(n) => {
				match &n.data {
					NodeData::Branches(b) => b.binary_search_by_key(&key, |b| b.key()),
					NodeData::Leaves(l) => l.binary_search_by_key(&key, |l| l.key()),
				}
			},
		};
		match result {
			Ok(i) => (i, true),
			// we didn't find the element, so point at the element just "before" the missing element
			Err(mut i) => {
				if i > 0 {
					i -= 1;
				};
				(i, false)
			},
		}
	}

	fn val(&self, index: usize) -> Option<Data> {
		match self {
			PageNode::Page(p) => {
				match p.page_type {
					Page::TYPE_LEAF => p.leaf_elements().get(index).map(|e| Data::from_leaf(e)),
					_ => panic!("INVALID PAGE TYPE FOR VAL"),
				}
			},
			PageNode::Node(n) => {
				match &n.data {
					NodeData::Leaves(l) => l.get(index).map(|l| l.clone()),
					_ => panic!("INVALID NODE TYPE FOR VAL"),
				}
			},
		}
	}
}

pub struct Cursor {
	bucket: Ptr<Bucket>,
	stack: Vec<Elem>,
}

impl Cursor {
	pub (crate) fn new(b: Ptr<Bucket>) -> Cursor {
		Cursor{
			bucket: b,
			stack: vec![],
			// phantom: std::marker::PhantomData{},
		}
	}

	pub (crate) fn current_id(&self) -> PageNodeID {
		let e = self.stack.last().unwrap();
		match &e.page_node {
			PageNode::Page(p) => PageNodeID::Page(p.id),
			PageNode::Node(n) => PageNodeID::Node(n.id),
		}
	}

	pub fn get<T: AsRef<[u8]>>(&mut self, key: T) -> Option<Data> {
		let exists = self.seek(key);
		if exists {
			self.current()
		} else {
			None
		}
	}

	// moves the cursor to a given point
	pub fn seek<T: AsRef<[u8]>>(&mut self, key: T) -> bool {
		self.stack.clear();
		self.search(key.as_ref(), self.bucket.meta.root_page)
	}

	pub fn current(&self) -> Option<Data> {
		match self.stack.last() {
			Some(e) => e.page_node.val(e.index),
			None => None,
		}
	}

	// recursive function that searches the bucket for a given key
	fn search(&mut self, key: &[u8], page_id: PageID) -> bool {
		let page_node = self.bucket.page_node(page_id);
		// println!("SEARCHING PAGEID {} for key {} with count {}", page_id, unsafe{std::str::from_utf8_unchecked(key)}, page_node.len());
		let (index, exact) = page_node.index(key);
		let leaf = page_node.leaf();
		self.stack.push(Elem{index, page_node});
		
		if leaf {
			return exact;
		}

		let next_page_id = self.stack.last().unwrap().page_node.index_page(index);
		if next_page_id == 0 {
			return false;
		}
		self.bucket.add_page_parent(next_page_id, page_id);

		self.search(key, next_page_id)
	}

	pub fn seek_first(&mut self) {
		if self.stack.len() == 0 {
			let page_node = self.bucket.page_node(self.bucket.meta.root_page);
			self.stack.push(Elem{index: 0, page_node});
		}
		loop {
			let elem = self.stack.last().unwrap();
			if elem.page_node.leaf() {
				break;
			}
			if elem.page_node.len() == 0 {
				break;
			}
			let page_node = self.bucket.page_node(elem.page_node.index_page(elem.index));
			self.stack.push(Elem{index: 0, page_node});
		}
	}
}

impl Iterator for Cursor {
    // we will be counting with usize
    type Item = Data;

    // next() is the only required method
    fn next(&mut self) -> Option<Self::Item> {
		if self.stack.len() == 0 {
			self.seek_first();
		} else {
			loop {
				let elem = self.stack.last_mut().unwrap();
				if elem.index >= (elem.page_node.len() - 1) {
					if self.stack.len() == 1 {
						return None;
					}
					self.stack.pop();
					continue;
				} else {
					elem.index += 1;
				}
				self.seek_first();
				break;
			}
		}
		self.current()
	}
}

struct Elem {
	index: usize,
	page_node: PageNode,
}