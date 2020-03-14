use std::pin::Pin;
use std::fs::File;
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::sync::{Arc, MutexGuard};

use memmap::Mmap;

use crate::db::{DBInner, MIN_ALLOC_SIZE};
use crate::meta::Meta;
use crate::page::{Page, PageID};
use crate::bucket::{Bucket};
use crate::errors::Result;
use crate::ptr::Ptr;
use crate::data::SliceParts;
use crate::errors::Error;
use crate::freelist::Freelist;

pub struct Transaction<'a> {
	inner: Pin<Box<TransactionInner>>,
	file: Option<MutexGuard<'a, File>>,
}

impl<'a> Transaction<'a> {
	pub (crate) fn new(db: &'a DBInner, writable: bool) -> Result<Transaction<'a>> {
		let file = if writable { Some(db.file.lock()?) } else { None };
		let tx = TransactionInner::new(db, writable)?;
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

	pub fn commit(mut self) -> Result<()> {
		if !self.inner.writable {
			return Err(Error::ReadOnlyTx);
		}
		self.inner.rebalance()?;
		self.inner.write_data(&mut self.file.as_mut().unwrap())
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
	pub (crate) writable: bool,
	pub (crate) freelist: Freelist,
	data: Arc<Mmap>,
	root: Option<Bucket>,
	buffers: Vec<Vec<u8>>,
}

impl<'a> TransactionInner {

	fn new(db: &DBInner, writable: bool) -> Result<TransactionInner> {
		let mut meta: Meta = db.meta();
		let mut freelist = db.freelist.clone();
		{
			let mut open_ro_txs = db.open_ro_txs.lock().unwrap();
			if writable {
				meta.tx_id += 1;
				if open_ro_txs.len() > 0 {
					freelist.release(open_ro_txs[0]);
				} else {
					freelist.release(meta.tx_id);
				}
			} else {
				open_ro_txs.push(meta.tx_id);
				open_ro_txs.sort_unstable();
			}
		}
		let _lock = db.mmap_lock.lock()?;
		let data = db.data.clone();

		let tx = TransactionInner{
			db: Ptr::new(db),
			meta,
			writable,
			freelist,
			data,
			root: None,
			buffers: Vec::new(),
		};
		Ok(tx)
	}

	fn init(&mut self) {
		let ptr = Ptr::new(self);
		self.root = Some(Bucket::root(ptr));
	}

	#[inline]
	pub (crate) fn page(&self, id: usize) -> &Page {
		Page::from_buf(&self.data, id, self.db.pagesize)
	}

	pub (crate) fn get_bucket(&'a mut self, name: &[u8]) -> Result<&'a mut Bucket> {
		debug_assert!(self.root.is_some());
		if let Some(root) = self.root.as_mut() {
			return root.get_bucket(name);
		}
		panic!("");
	}

	pub (crate) fn create_bucket(&'a mut self, name: &[u8]) -> Result<&'a mut Bucket> {
		debug_assert!(self.root.is_some());
		if let Some(root) = self.root.as_mut() {
			return root.create_bucket(name);
		}
		panic!("");
	}

	pub (crate) fn copy_data(&mut self, data: &[u8]) -> SliceParts {
		let data = Vec::from(data);
		self.buffers.push(data);
		SliceParts::from_slice(&self.buffers.last().unwrap()[..])
	}

	pub (crate) fn free(&mut self, page_id: PageID, num_pages: usize) {
		for id in page_id..(page_id+num_pages) {
			self.freelist.free(self.meta.tx_id, id);
		}
	}

	pub (crate) fn allocate(&mut self, bytes: usize) -> (PageID, usize) {
		let num_pages = (bytes / self.db.pagesize) + 1;
		let page_id = match self.freelist.allocate(num_pages) {
			Some(page_id) => page_id,
			None => {
				let page_id = self.meta.num_pages + 1;
				self.meta.num_pages += num_pages;
				page_id
			}
		};
		(page_id, num_pages)
	}

	fn rebalance(&mut self) -> Result<()> {
		let root = self.root.as_mut().unwrap();
		root.rebalance()?;		
		Ok(())
	}

	fn write_data(&mut self, file: &mut File) -> Result<()> {
		let required_size = (self.meta.num_pages * self.db.pagesize) as u64;
		let current_size = file.metadata()?.len();
		if current_size < required_size {
			let size_diff = required_size - current_size;
			let alloc_size = ((size_diff / MIN_ALLOC_SIZE) + 1) * MIN_ALLOC_SIZE;
			self.db.resize(file, current_size + alloc_size)?;
		}

		// write the data to the file
		{
			let root = self.root.as_mut().unwrap();
			root.write(file)?;
			self.meta.root = root.meta;
		}

		// write freelist to file
		{
			self.freelist.free(self.meta.tx_id, self.meta.freelist_page);
			let (page_id, num_pages) = self.allocate(self.freelist.size());
			let page_ids = self.freelist.pages();
			let size = self.freelist.size();

			let mut buf = vec![0; size];

			#[allow(clippy::cast_ptr_alignment)]
			let mut page = unsafe {&mut *(&mut buf[0] as *mut u8 as *mut Page)};
						
			page.id = page_id;
			page.overflow = num_pages - 1;
			page.page_type = Page::TYPE_FREELIST;
			page.count = page_ids.len();
			page.freelist_mut().copy_from_slice(page_ids.as_slice());

			file.write_all_at(buf.as_slice(), (self.db.pagesize * page_id) as u64)?;

			self.meta.freelist_page = page_id;
		}

		// write meta page to file
		{
			let mut buf = vec![0; self.db.pagesize];

			#[allow(clippy::cast_ptr_alignment)]
			let mut page = unsafe {&mut *(&mut buf[0] as *mut u8 as *mut Page)};
			
			let meta_page_id = if self.meta.meta_page == 0 { 1 } else { 0 };
			
			page.id = meta_page_id;
			page.page_type = Page::TYPE_META;
			let m = page.meta_mut();
			
			m.meta_page = meta_page_id as u8;
			m.magic = self.meta.magic;
			m.version = self.meta.version;
			m.pagesize = self.meta.pagesize;
			m.root = self.meta.root;
			m.num_pages = self.meta.num_pages;
			m.freelist_page = self.meta.freelist_page;
			m.tx_id = self.meta.tx_id;
			m.hash = m.hash_self();
			file.write_all_at(buf.as_slice(), (self.db.pagesize * meta_page_id) as u64)?;
		}

		file.flush()?;
		file.sync_all()?;

		self.db.freelist = self.freelist.clone();
		Ok(())
	}

}

impl Drop for TransactionInner {
    fn drop(&mut self) {
		if !self.writable {
			let mut open_txs = self.db.open_ro_txs.lock().unwrap();
			let index = match open_txs.binary_search(&self.meta.tx_id) {
				Ok(i) => i,
				_ => {
					debug_assert!(false, "dropped transaction id does not exist");
					return;
				},
			};
			open_txs.remove(index);
		}
    }
}