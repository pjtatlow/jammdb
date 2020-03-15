use std::collections::HashMap;
use std::fs::File;
use std::pin::Pin;

use crate::cursor::{Cursor, PageNode, PageNodeID};
use crate::data::{BucketData, Data, KVPair};
use crate::errors::{Error, Result};
use crate::node::{Branch, Node, NodeData, NodeID};
use crate::page::{Page, PageID};
use crate::ptr::Ptr;
use crate::transaction::TransactionInner;

pub struct Bucket {
	pub(crate) tx: Ptr<TransactionInner>,
	pub(crate) meta: BucketMeta,
	pub(crate) root: PageNodeID,
	dirty: bool,
	// page: Option<Ptr<Page>>,
	// node: Option<NodeID>,
	buckets: HashMap<Vec<u8>, Pin<Box<Bucket>>>,
	nodes: Vec<Pin<Box<Node>>>,
	page_node_ids: HashMap<PageID, NodeID>,
	page_parents: HashMap<PageID, PageID>,
}

impl Bucket {
	pub(crate) fn root(tx: Ptr<TransactionInner>) -> Bucket {
		let meta = tx.meta.root;
		Bucket {
			tx,
			meta,
			root: PageNodeID::Page(meta.root_page),
			dirty: false,
			buckets: HashMap::new(),
			nodes: Vec::new(),
			page_node_ids: HashMap::new(),
			page_parents: HashMap::new(),
		}
	}

	fn new_child(&mut self, name: &[u8]) {
		let b = Bucket {
			tx: Ptr::new(&self.tx),
			meta: BucketMeta::default(),
			root: PageNodeID::Node(0),
			dirty: true,
			buckets: HashMap::new(),
			nodes: Vec::new(),
			page_node_ids: HashMap::new(),
			page_parents: HashMap::new(),
		};
		self.buckets.insert(Vec::from(name), Pin::new(Box::new(b)));
		let b = self.buckets.get_mut(name).unwrap();
		let n = Node::new(0, Page::TYPE_LEAF, Ptr::new(b));

		b.nodes.push(Pin::new(Box::new(n)));
		b.page_node_ids.insert(0, 0);
	}

	pub(crate) fn new_node(&mut self, data: NodeData) -> &mut Node {
		let node_id = self.nodes.len();
		let n = Node::with_data(node_id, data, Ptr::new(self));
		self.nodes.push(Pin::new(Box::new(n)));
		self.nodes.get_mut(node_id).unwrap()
	}

	fn from_meta(&self, meta: BucketMeta) -> Bucket {
		Bucket {
			tx: Ptr::new(&self.tx),
			meta,
			root: PageNodeID::Page(meta.root_page),
			dirty: false,
			buckets: HashMap::new(),
			nodes: Vec::new(),
			page_node_ids: HashMap::new(),
			page_parents: HashMap::new(),
		}
	}

	pub fn get_bucket<T: AsRef<[u8]>>(&mut self, name: T) -> Result<&mut Bucket> {
		let name = name.as_ref();
		let key = Vec::from(name);
		if !self.buckets.contains_key(&key) {
			let mut c = self.cursor();
			let exists = c.seek(name);
			if !exists {
				return Err(Error::BucketMissing);
			}
			match c.current() {
				Some(data) => match data {
					Data::Bucket(data) => {
						let mut b = self.from_meta(data.meta());
						b.meta = data.meta();
						b.dirty = false;
						self.buckets.insert(key.clone(), Pin::new(Box::new(b)));
					}
					_ => return Err(Error::IncompatibleValue),
				},
				None => return Err(Error::BucketMissing),
			}
		}
		Ok(self.buckets.get_mut(&key).unwrap())
	}

	pub fn create_bucket<T: AsRef<[u8]>>(&mut self, name: T) -> Result<&mut Bucket> {
		if !self.tx.writable {
			return Err(Error::ReadOnlyTx);
		}
		self.dirty = true;
		let mut c = self.cursor();
		let name = name.as_ref();
		let exists = c.seek(name);
		if exists {
			return Err(Error::BucketExists);
		}
		self.meta.next_int += 1;
		let key = Vec::from(name);
		self.new_child(&key);

		let data;
		{
			let b = self.buckets.get(&key).unwrap();
			let key = self.tx.copy_data(name);
			data = Data::Bucket(BucketData::from_meta(key, &b.meta));
		}

		let node = self.node(c.current_id());
		node.insert_data(data);
		let b = self.buckets.get_mut(&key).unwrap();
		Ok(b)
	}

	pub fn next_int(&self) -> u64 {
		self.meta.next_int
	}

	pub fn get<T: AsRef<[u8]>>(&self, key: T) -> Option<Data> {
		let mut c = self.cursor();
		let exists = c.seek(key);
		if exists {
			c.current()
		} else {
			None
		}
	}

	// Returns an Error only if the current transaction is read-only.
	pub fn put<T: AsRef<[u8]>, S: AsRef<[u8]>>(&mut self, key: T, value: S) -> Result<()> {
		if !self.tx.writable {
			return Err(Error::ReadOnlyTx);
		}
		let k = self.tx.copy_data(key.as_ref());
		let v = self.tx.copy_data(value.as_ref());
		self.put_data(Data::KeyValue(KVPair::from_slice_parts(k, v)));
		Ok(())
	}

	fn put_data(&mut self, data: Data) {
		self.dirty = true;
		let mut c = self.cursor();
		let exists = c.seek(data.key());
		if !exists {
			self.meta.next_int += 1;
		}
		let node = self.node(c.current_id());
		node.insert_data(data);
	}

	pub fn cursor(&self) -> Cursor {
		Cursor::new(Ptr::new(self))
	}

	pub(crate) fn page_node(&self, page: PageID) -> PageNode {
		if let Some(node_id) = self.page_node_ids.get(&page) {
			PageNode::Node(Ptr::new(self.nodes.get(*node_id).unwrap()))
		} else {
			PageNode::Page(Ptr::new(self.tx.page(page)))
		}
	}

	pub(crate) fn add_page_parent(&mut self, page: PageID, parent: PageID) {
		debug_assert!(self.meta.root_page == parent || self.page_parents.contains_key(&parent));
		self.page_parents.insert(page, parent);
	}

	pub(crate) fn node(&mut self, id: PageNodeID) -> &mut Node {
		let id: NodeID = match id {
			PageNodeID::Page(page_id) => {
				if let Some(node_id) = self.page_node_ids.get(&page_id) {
					return &mut self.nodes[*node_id as usize];
				}
				debug_assert!(
					self.meta.root_page == page_id || self.page_parents.contains_key(&page_id)
				);
				let node_id = self.nodes.len();
				self.page_node_ids.insert(page_id, node_id);
				let n: Node = Node::from_page(node_id, Ptr::new(self), self.tx.page(page_id));
				self.nodes.push(Pin::new(Box::new(n)));
				if self.meta.root_page != page_id {
					let node_key = self.nodes[node_id].data.key_parts();
					let parent = self.node(PageNodeID::Page(self.page_parents[&page_id]));
					parent.insert_child(node_id, node_key);
				}
				node_id
			}
			PageNodeID::Node(id) => id,
		};
		self.nodes.get_mut(id).unwrap()
	}

	pub(crate) fn rebalance(&mut self) -> Result<BucketMeta> {
		let mut bucket_metas = HashMap::new();
		for (key, b) in self.buckets.iter_mut() {
			if b.dirty {
				self.dirty = true;
				let bucket_meta = b.rebalance()?;
				bucket_metas.insert(key.clone(), bucket_meta);
			}
		}
		for (k, b) in bucket_metas {
			let name = self.tx.copy_data(&k[..]);
			let meta = self.tx.copy_data(b.as_ref());
			self.put_data(Data::Bucket(BucketData::from_slice_parts(name, meta)));
		}
		let mut root_id = self.root;
		if self.dirty {
			let mut root_node = self.node(self.root);
			root_node.merge();
			while let Some(mut branches) = root_node.split() {
				branches.insert(0, Branch::from_node(root_node));
				root_node = self.new_node(NodeData::Branches(branches));
			}
			root_id = PageNodeID::Node(root_node.id);
			self.meta.root_page = root_node.page_id;
		}
		self.root = root_id;
		Ok(self.meta)
	}

	pub(crate) fn write(&mut self, file: &mut File) -> Result<()> {
		for (_, b) in self.buckets.iter_mut() {
			b.write(file)?;
		}
		if self.dirty {
			for node in self.nodes.iter_mut() {
				node.write(file)?;
			}
		}
		Ok(())
	}

	pub fn print(&self) {
		let page = self.tx.page(self.meta.root_page);
		page.print(&self.tx);
	}
}

const META_SIZE: usize = std::mem::size_of::<BucketMeta>();

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct BucketMeta {
	pub(crate) root_page: PageID,
	pub(crate) next_int: u64,
}

impl AsRef<[u8]> for BucketMeta {
	#[inline]
	fn as_ref(&self) -> &[u8] {
		let ptr = self as *const BucketMeta as *const u8;
		unsafe { std::slice::from_raw_parts(ptr, META_SIZE) }
	}
}
