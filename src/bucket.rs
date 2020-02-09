use std::collections::HashMap;
use std::hash::Hasher;
use std::fs::File;
use std::pin::Pin;

use crate::page::{Page, PageID, PageType};
use crate::node::{Node, NodeID, NodeData};
use crate::transaction::TransactionInner;
use crate::ptr::Ptr;
use crate::cursor::{Cursor, PageNode, PageNodeID};
use crate::errors::{Error, Result};
use crate::data::{Data, BucketData, KVPair};

pub struct Bucket {
	pub (crate) tx: Ptr<TransactionInner>,
	pub (crate) meta: BucketMeta,
	pub (crate) root: PageNodeID,
	dirty: bool,
	// page: Option<Ptr<Page>>,
	// node: Option<NodeID>,
	buckets: HashMap<Vec<u8>, Pin<Box<Bucket>>>,
	nodes: Vec<Box<Node>>,
	page_node_ids: HashMap<PageID, NodeID>,
	page_parents: HashMap<PageID, PageID>,
}

impl Bucket {
	pub (crate) fn root(tx: Ptr<TransactionInner>) -> Bucket {
		// println!("A: {:?} {:?}", tx.0, tx.meta.root);
		let meta = tx.meta.root.clone();
		// println!("B: {:?} {:?}", tx.0, meta);
		Bucket{
			tx,
			meta,
			root: PageNodeID::Page(meta.root_page),
			dirty: false,
			// page: None,
			// node: None,
			buckets: HashMap::new(),
			nodes: Vec::new(),
			page_node_ids: HashMap::new(),
			page_parents: HashMap::new(),
		}
	}

	fn new_child(&mut self, name: &Vec<u8>) -> &mut Bucket {
		let b = Bucket{
			tx: Ptr::new(&self.tx),
			meta: BucketMeta::default(),
			root: PageNodeID::Node(0),
			dirty: true,
			buckets: HashMap::new(),
			nodes: Vec::new(),
			page_node_ids: HashMap::new(),
			page_parents: HashMap::new(),
		};
		self.buckets.insert(name.clone(), Pin::new(Box::new(b)));
		let b = self.buckets.get_mut(name).unwrap();
		
		let n = Node::new(0, Page::TYPE_LEAF, Ptr::new(b));

		b.nodes.push(Box::new(n));
		b.page_node_ids.insert(0,0);
		b
	}

	pub (crate) fn new_node(&mut self, data: NodeData) -> &mut Node {
		let node_id = self.nodes.len();
		let n = Node::with_data(node_id, data, Ptr::new(self));
		self.nodes.push(Box::new(n));
		self.nodes.get_mut(node_id).unwrap()
	}

	fn from_meta(&self, meta: BucketMeta) -> Bucket {
		Bucket{
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
		// if let Some(b) = self.buckets.get_mut(&key) {
		// 	return Ok(b);
		// }
		let mut c = self.cursor();
		c.seek(name);
		match c.current() {
			Some(data) => match data {
				Data::Bucket(data) => {
					let mut b = self.from_meta(data.meta());
					b.meta = data.meta();
					b.dirty = false;
					// println!("NEW BUCKET META: {:?}", b.meta);
					self.buckets.insert(key.clone(), Pin::new(Box::new(b)));
					Ok(self.buckets.get_mut(&key).unwrap())
				},
				_ => Err(Error::IncompatibleValue) 
			},
			None => Err(Error::BucketMissing)
		}
	}

	pub fn create_bucket<T: AsRef<[u8]>>(&mut self, name: T) -> Result<&mut Bucket> {
		self.dirty = true;
		let mut c = self.cursor();
		let name = name.as_ref();
		let exists = c.seek(name);
		if exists {
			return Err(Error::BucketExists);
		}
		let key = Vec::from(name);
		self.new_child(&key);
		// TODO avoid cloning the key
		// self.buckets.insert(key.clone(), Box::new(b));
		let data;
		{
			let b = self.buckets.get(&key).unwrap();
			let key = self.tx.copy_data(name);
			data = BucketData::from_bucket(key, b);
			// match &data {
			// 	Data::Bucket(b) => println!("CREATED BUCKET: {:?}", b.meta()),
			// 	_ => (),
			// };
		}
		// if let Data::Bucket(p) = data {
			// 	println!("OG META: {:?}", p.meta());
			// 	b.meta.sequence = 2871348274;
			// 	println!("NEW META: {:?}", p.meta());
			// }
		{
			let node = self.node(c.current_id());
			node.insert_data(data);
		}
		let b = self.buckets.get_mut(&key).unwrap();
		Ok(b)
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

	pub fn put<T: AsRef<[u8]>>(&mut self, key: T, value: T) {
		self.dirty = true;
		let mut c = self.cursor();
		let key = key.as_ref();
		let value = value.as_ref();
		c.seek(key);
		let k = self.tx.copy_data(key);
		let v = self.tx.copy_data(value);
		let node = self.node(c.current_id());
		node.insert_data(KVPair::from_slice_parts(k, v));
	}

	pub fn cursor(&self) -> Cursor {
		Cursor::new(Ptr::new(self))
	}

	pub (crate) fn page_node(&self, page: PageID) -> PageNode {
		if let Some(node_id) = self.page_node_ids.get(&page) {
			PageNode::Node(Ptr::new(self.nodes.get(*node_id).unwrap()))
		} else {
			PageNode::Page(Ptr::new(self.tx.page(page)))
		}
	}

	pub (crate) fn add_page_parent(&mut self, page: PageID, parent: PageID) {
		debug_assert!(self.meta.root_page == parent || self.page_parents.contains_key(&parent));
		self.page_parents.insert(page, parent);
	}

	pub (crate) fn node(&mut self, id: PageNodeID) -> &mut Node {
		let id: NodeID = match id {
			PageNodeID::Page(page_id) => {
				if let Some(node_id) = self.page_node_ids.get(&page_id) {
					return &mut self.nodes[*node_id as usize];
				}
				debug_assert!(self.meta.root_page == page_id || self.page_parents.contains_key(&page_id));
				let node_id = self.nodes.len();
				self.page_node_ids.insert(page_id, node_id);
				let mut n: Node = Node::from_page(node_id, Ptr::new(self), self.tx.page(page_id));
				if self.meta.root_page != page_id {
					let parent = self.node(PageNodeID::Page(self.page_parents[&page_id]));
					parent.insert_branch(n.id, n.data.key_parts());
					n.parent = Some(PageNodeID::Node(parent.id));
				}
				self.nodes.push(Box::new(n));
				node_id
			},
			PageNodeID::Node(id) => id,
		};
		self.nodes.get_mut(id).unwrap()
	}

	pub (crate) fn rebalance(&mut self) -> Result<()> {
		for (_, b) in self.buckets.iter_mut() {
			b.rebalance()?;
		};
		if self.dirty {
			let root_node = self.node(self.root);
			root_node.merge();
			root_node.split();
		}
		Ok(())
	}

	pub (crate) fn write(&mut self, file: &mut File) -> Result<()> {
		for (_, b) in self.buckets.iter_mut() {
			b.write(file)?;
		};
		if self.dirty {
			let page_id = self.node(self.root).write(file)?;
			self.meta.root_page = page_id;
		}
		Ok(())
	}

	pub fn print(&self) {
		let page = self.tx.page(self.meta.root_page);
		page.print(&self.tx);
	}
}

// impl IntoIterator for Bucket {
//     type Item = <Cursor as Iterator>::Item;
//     type IntoIter = Cursor;

//     fn into_iter(self) -> Self::IntoIter {
//         self.cursor()
//     }
// }

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub (crate) struct BucketMeta {
	pub (crate) root_page: PageID,
	pub (crate) sequence: u64,
}