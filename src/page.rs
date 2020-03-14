use std::slice::{from_raw_parts, from_raw_parts_mut};
use std::mem::size_of;
use std::io::Write;

use crate::meta::Meta;
use crate::node::{Node, NodeType, NodeData};
use crate::errors::Result;
use crate::transaction::TransactionInner;
use crate::data::SliceParts;
use crate::bucket::BucketMeta;

pub (crate) type PageID = usize;

pub (crate) type PageType = u8;

#[repr(C)]
#[derive(Debug)]
pub (crate) struct Page {
	pub (crate) id: PageID,
	pub (crate) page_type: PageType,
	pub (crate) count: usize,
	pub (crate) overflow: usize,
	pub (crate) ptr: usize,
}

impl Page {
	pub (crate) const TYPE_BRANCH: PageType = 0x01;
	pub (crate) const TYPE_LEAF: PageType = 0x02;
	pub (crate) const TYPE_META: PageType = 0x03;
	pub (crate) const TYPE_FREELIST: PageType = 0x04;

	#[inline]
	pub (crate) fn from_buf(buf: &[u8], id: PageID, pagesize: usize) -> &Page {
		#[allow(clippy::cast_ptr_alignment)]
		unsafe {&*(&buf[id * pagesize] as *const u8 as *const Page)}
	}

	pub (crate) fn meta(&self) -> &Meta {
		// assert_type(self.page_type, Page::TYPE_META);
		unsafe{&*(&self.ptr as *const usize as *const Meta)}
	}
	
	pub (crate) fn meta_mut(&mut self) -> &mut Meta {
		// assert_type(self.page_type, Page::TYPE_META);
		unsafe{&mut *(&mut self.ptr as *mut usize as *mut Meta)}
	}

	pub (crate) fn freelist(&self) -> &[PageID] {
		assert_type(self.page_type, Page::TYPE_FREELIST);
		unsafe{
			let start = &self.ptr as *const usize as *const PageID;
			from_raw_parts(start, self.count as usize)
		}		
	}

	pub (crate) fn freelist_mut(&mut self) -> &mut [PageID] {
		assert_type(self.page_type, Page::TYPE_FREELIST);
		unsafe{
			let start = &self.ptr as *const usize as *mut PageID;
			from_raw_parts_mut(start, self.count as usize)
		}		
	}

	pub (crate) fn leaf_elements(&self) -> &[LeafElement] {
		assert_type(self.page_type, Page::TYPE_LEAF);
		unsafe{
			let start = &self.ptr as *const usize as *const LeafElement;
			from_raw_parts(start, self.count as usize)
		}		
	}

	pub (crate) fn branch_elements(&self) -> &[BranchElement] {
		assert_type(self.page_type, Page::TYPE_BRANCH);
		unsafe{
			let start = &self.ptr as *const usize as *const BranchElement;
			from_raw_parts(start, self.count as usize)
		}
	}

	pub (crate) fn leaf_elements_mut(&mut self) -> &mut[LeafElement] {
		assert_type(self.page_type, Page::TYPE_LEAF);
		unsafe{
			let start = &self.ptr as *const usize as *const LeafElement as *mut LeafElement;
			from_raw_parts_mut(start, self.count as usize)
		}		
	}

	pub (crate) fn branch_elements_mut(&mut self) -> &mut[BranchElement] {
		assert_type(self.page_type, Page::TYPE_BRANCH);
		unsafe{
			let start = &self.ptr as *const usize as *const BranchElement as *mut BranchElement;
			from_raw_parts_mut(start, self.count as usize)
		}
	}

	fn slice(&mut self, size: usize) -> &mut [u8] {
		unsafe{
			let start = &self.ptr as *const usize as *const u8 as *mut u8;
			from_raw_parts_mut(start, size)
		}
	}

	pub (crate) fn write_node(&mut self, n: &Node, num_pages: usize) -> Result<()> {
		self.id = n.page_id;
		self.count = n.data.len();
		self.overflow = num_pages - 1;
		let header_size;
		let mut data_size = 0;
		let mut data: Vec<&[u8]>;
		match &n.data {
			NodeData::Branches(branches) => {
				self.page_type = Page::TYPE_BRANCH;
				header_size = size_of::<BranchElement>();
				let mut header_offsets = header_size * branches.len();
				data = Vec::with_capacity(self.count);
				let elems = self.branch_elements_mut();
				for (b, elem) in branches.iter().zip(elems.iter_mut()) {
					debug_assert!(b.page != 0, "PAGE SHOULD NOT BE ZERO!");
					elem.page = b.page;
					elem.key_size = b.key_size();
					elem.pos = header_offsets + data_size;
					data_size += elem.key_size;
					header_offsets -= header_size;
					data.push(b.key());
				};
			},
			NodeData::Leaves(leaves) => {
				self.page_type = Page::TYPE_LEAF;
				header_size = size_of::<LeafElement>();
				let mut header_offsets = header_size * leaves.len();
				data = Vec::with_capacity(self.count * 2);
				let elems = self.leaf_elements_mut();
				for (l, elem) in leaves.iter().zip(elems.iter_mut()) {
					elem.node_type = l.node_type();

					let key = l.key();
					let value = l.value();
					elem.key_size = key.len();
					elem.value_size = value.len();
					elem.pos = header_offsets + data_size;

					data_size += elem.key_size + elem.value_size;
					header_offsets -= header_size;

					data.push(key);
					data.push(value);
				};				
			},
		};
		let total_header = header_size * self.count;
		let buf = self.slice(total_header + data_size);
		let mut buf = &mut buf[total_header..];
		for b in data.iter() {
			buf.write_all(b)?;
		}
		Ok(())
	}

	pub fn print(&self, tx: &TransactionInner) {
		let name = self.name();
		println!("{} [style=\"filled\", fillcolor=\"darkorchid1\"];", name);
		match self.page_type {
			Page::TYPE_BRANCH => {
				for (i, elem) in self.branch_elements().iter().enumerate() {
					let key = elem.key();
					let elem_name = match std::str::from_utf8(key) {
						// Ok(key) => format!("\"Index: {}\\nPage: {}\\nKey: '{}'\"", i, self.id, key),
						_ => format!("\"Index: {}\\nPage: {}\\nKey: {:?}\"", i, self.id, key),
					};
					let page = tx.page(elem.page);
					println!("{} [style=\"filled\", fillcolor=\"burlywood\"];", elem_name);
					println!("{} -> {}", name, elem_name);
					println!("{} -> {}", elem_name, page.name());
					page.print(tx);
				}
			},
			Page::TYPE_LEAF => {
				for (i, elem) in self.leaf_elements().iter().enumerate() {
					match elem.node_type {
						Node::TYPE_BUCKET => {
							let parts = SliceParts::from_slice(elem.value());
							let meta = unsafe{ *(parts.slice()[0] as *const BucketMeta) };
							let elem_name = match std::str::from_utf8(elem.key()) {
								// Ok(key) => format!("\"Index: {}\\nPage: {}\\nKey '{}'\\n {:?}\"", i, self.id, key, meta),
								_ => format!("\"Index: {}\\nPage: {}\\nKey {:?}\\n {:?}\"", i, self.id, elem.key(), meta),	
							};
							println!("{} [style=\"filled\", fillcolor=\"gray91\"];", elem_name);
							println!("{} -> {}", name, elem_name);
							let page = tx.page(meta.root_page);
							println!("{} -> {}", elem_name, page.name());
							page.print(tx);
						},
						Node::TYPE_DATA => {
							// return;
							let elem_name = format!("\"Index: {}\\nPage: {}\\nKey: {:?}\\nValue '{}'\"", i, self.id, elem.key(), std::str::from_utf8(elem.value()).unwrap());
							// let elem_name = format!("\"Index: {}\\nPage: {}\\nKey: '{}'\\nValue '{}'\"", i, self.id, std::str::from_utf8(elem.key()).unwrap(), std::str::from_utf8(elem.value()).unwrap());
							println!("{} [style=\"filled\", fillcolor=\"chartreuse\"];", elem_name);
							println!("{} -> {}", name, elem_name);
						},
						_ => panic!("LOL NOPE"),
					}
				}
			},
			_ => panic!("CANNOT WRITE NODE OF TYPE {} from page {}", type_str(self.page_type), self.id),
		}
	}

	pub fn name(&self) -> String {
		let size = 4096 + (self.overflow * 4096);
		format!("\"Page #{} ({}) ({} bytes)\"", self.id, type_str(self.page_type), size)
	}

}

fn type_str(pt: PageType) -> String {
	match pt {
		Page::TYPE_BRANCH => String::from("Branch"),
		Page::TYPE_FREELIST => String::from("FreeList"),
		Page::TYPE_LEAF => String::from("Leaf"),
		Page::TYPE_META => String::from("Meta"),
		_ => format!("Invalid ({:#X})", pt),
	}	
}

fn assert_type(actual: PageType, expected: PageType) {
	if actual != expected {
		panic!(format!("expected page type \"{}\" but got \"{}\"", type_str(expected), type_str(actual)));
	}
}

#[repr(C)]
pub (crate) struct BranchElement {
	pub (crate) page: PageID,
	key_size: usize,
	pos: usize,
}

impl BranchElement {
	pub (crate) fn key(&self) -> &[u8] {
		let pos = self.pos;
		unsafe {
			let start = self as *const BranchElement as *const u8;
			let buf = std::slice::from_raw_parts(start, pos + self.key_size);
			&buf[pos..]
		}
	}
}

#[repr(C)]
pub (crate) struct LeafElement {
	pub (crate) node_type: NodeType,
	pos: usize,
	key_size: usize,
	value_size: usize,
}

impl LeafElement {
	pub (crate) fn key(&self) -> &[u8] {
		let pos = self.pos;
		unsafe {
			let start = self as *const LeafElement as *const u8;
			let buf = std::slice::from_raw_parts(start, pos + self.key_size);
			&buf[pos..]
		}
	}
	pub (crate) fn value(&self) -> &[u8] {
		let pos = self.pos + self.key_size;
		unsafe {
			let start = self as *const LeafElement as *const u8;
			let buf = std::slice::from_raw_parts(start, pos + self.value_size);
			&buf[pos..]
		}
	}
}