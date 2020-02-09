use std::pin::Pin;
use std::sync::MutexGuard;
use std::fs::File;
use std::os::unix::fs::FileExt;

use crate::db::{DBInner};
use crate::meta::Meta;
use crate::page::{Page, PageID};
use crate::bucket::{Bucket};
use crate::errors::Result;
use crate::ptr::Ptr;
use crate::data::SliceParts;

pub struct Transaction<'a> {
	inner: Pin<Box<TransactionInner>>,
	file: MutexGuard<'a, File>,
}

impl<'a> Transaction<'a> {
	pub (crate) fn new(db: &'a DBInner) -> Result<Transaction<'a>> {
		let file = db.file.lock()?;
		let tx = TransactionInner::new(db)?;
		let mut inner = Pin::new(Box::new(tx));
		inner.init();
		Ok(Transaction{
			inner,
			file,
		})
	}

	pub fn get_bucket<T: AsRef<[u8]>>(&mut self, name: T) -> Result<&mut Bucket> {
		self.inner.get_bucket(name.as_ref())
	}

	pub fn create_bucket<T: AsRef<[u8]>>(&mut self, name: T) -> Result<&mut Bucket> {
		self.inner.create_bucket(name.as_ref())
	}

	pub fn commit(&mut self) -> Result<()> {
		self.inner.rebalance();
		self.inner.write_data(&mut self.file)
	}

	pub fn print_graph(&self) {
		println!("digraph G {{");
		self.inner.root.as_ref().unwrap().print();
		println!("}}");
	}
}

pub (crate) struct TransactionInner {
	pub (crate) db: Ptr<DBInner>,
	pub (crate) meta: Meta,
	// _write_lock: std::sync::MutexGuard<, ()>,
	root: Option<Bucket>,
	buffers: Vec<Vec<u8>>,
	// phantom: std::marker::PhantomData<&'tx u8>,
}

impl<'a> TransactionInner {

	fn new(db: &DBInner) -> Result<TransactionInner> {
		let _write_lock = db.write_lock.lock()?;
		// let db2 = db.clone();
		let meta: Meta = Page::from_buf(&db.data, 0, db.pagesize).meta().clone();
		// println!("{:?}", meta);
		let tx = TransactionInner{
			db: Ptr::new(db),
			meta,
			// _write_lock,
			root: None,
			buffers: Vec::new(),
			// phantom: std::marker::PhantomData{},
		};
		// println!("ID({:?}): {:?}", std::thread::current().id(), tx.page(0).meta());
		Ok(tx)
	}

	fn init(&mut self) {
		let ptr = Ptr::new(self);
		self.root = Some(Bucket::root(ptr));
	}

	#[inline]
	pub (crate) fn page(&self, id: usize) -> &Page {
		Page::from_buf(&self.db.data, id, self.db.pagesize)
	}

	pub (crate) fn get_bucket(&'a mut self, name: &[u8]) -> Result<&'a mut Bucket> {
		if let Some(mut root) = self.root.as_mut() {
			return root.get_bucket(name);
		}
		panic!("");
	}

	pub (crate) fn create_bucket(&'a mut self, name: &[u8]) -> Result<&'a mut Bucket> {
		if let Some(mut root) = self.root.as_mut() {
			return root.create_bucket(name);
		}
		panic!("");
	}

	pub (crate) fn copy_data(&mut self, data: &[u8]) -> SliceParts {
		let data = Vec::from(data);
		self.buffers.push(data);
		SliceParts::from_slice(&self.buffers.last().unwrap()[..])
	}

	pub (crate) fn allocate(&mut self, bytes: usize) -> (PageID, usize) {
		let num_pages = (bytes / self.db.pagesize) + 1;
		let page_id = self.meta.num_pages + 1;
		self.meta.num_pages += num_pages;
		(page_id, num_pages)
	}

	fn rebalance(&mut self) -> Result<()> {
		let root = self.root.as_mut().unwrap();
		root.rebalance()?;		
		Ok(())
	}

	fn write_data(&mut self, file: &mut File) -> Result<()> {
		let root = self.root.as_mut().unwrap();
		root.write(file)?;
		self.meta.root = root.meta;
		let mut buf = vec![0; self.db.pagesize];
		let mut page = unsafe {&mut *(&mut buf[0] as *mut u8 as *mut Page)};
		page.id = 0;
		page.page_type = Page::TYPE_META;
		let m = page.meta_mut();
		
		m.magic = self.meta.magic;
		m.version = self.meta.version;
		m.pagesize = self.meta.pagesize;
		m.freelist_page = self.meta.freelist_page;
		m.root = self.meta.root;
		m.num_pages = self.meta.num_pages;
		// let start = &self.meta as *const Meta as *const u8;
		// let buf = unsafe{ std::slice::from_raw_parts(start, std::mem::size_of::<Meta>()) };
		file.write_all_at(buf.as_slice(), 0)?;
		// self.db.remap(file);
		Ok(())
	}

}