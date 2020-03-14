use std::io::Write;
use std::fs::{File, OpenOptions as FileOpenOptions};
use std::sync::{Arc, Mutex};
use std::path::Path;

use fs2::FileExt;
use memmap::Mmap;
use page_size::{get as getPageSize};

use crate::meta::{Meta};
use crate::page::{Page};
use crate::errors::{Result};
use crate::bucket::{BucketMeta};
use crate::transaction::Transaction;
use crate::freelist::Freelist;

const MAGIC_VALUE: u32 = 0xABCDEF;
const VERSION: u32 = 1;

// Minimum number of bytes to allocate when growing the databse
pub (crate) const MIN_ALLOC_SIZE: u64 = 8 * 1024 * 1024;

// Number of pages to allocate when creating the database
const DEFAULT_NUM_PAGES: usize = 32;

pub struct OpenOptions {
	pagesize: usize,
	num_pages: usize,
}

impl OpenOptions {
	pub fn new() -> OpenOptions {
		let pagesize = getPageSize();
		OpenOptions{
			pagesize,
			num_pages: DEFAULT_NUM_PAGES,
		}
	}

	pub fn pagesize(mut self, pagesize: usize) -> OpenOptions {
		self.pagesize = pagesize;
		self
	}

	pub fn num_pages(mut self, num_pages: usize) -> OpenOptions {
		if num_pages < 4 {
			panic!("Must have 4 or more pages minimum");
		}
		self.num_pages = num_pages;
		self
	}

	pub fn open<P: AsRef<Path>>(self, path: P) -> Result<DB> {
		let path: &Path = path.as_ref();
		let file = if !path.exists() {
			init_file(path, self.pagesize, self.num_pages)?
		} else {
			FileOpenOptions::new()
				.read(true)
				.write(true)
				.open(path)?
		};

		let db = DBInner::open(file, self.pagesize)?;
		Ok(DB(Arc::new(db)))			
	}
	
}

#[derive(Clone)]
pub struct DB(Arc<DBInner>);

impl DB {
	pub fn open<P: AsRef<Path>>(path: P) -> Result<DB> {
		OpenOptions::new().open(path)
	}

	pub fn tx(&mut self, writable: bool) -> Result<Transaction> {
		Transaction::new(&self.0, writable)
	}

	pub fn pagesize(&self) -> usize {
		self.0.pagesize
	}
}

pub (crate) struct DBInner {
	pub (crate) data: Arc<Mmap>,
	pub (crate) freelist: Freelist,

	pub (crate) file: Mutex<File>,
	pub (crate) mmap_lock: Mutex<()>,
	pub (crate) open_ro_txs: Mutex<Vec<u64>>,

	pub (crate) pagesize: usize,
}

impl DBInner {

	pub (crate) fn open(file: File, pagesize: usize) -> Result<DBInner> {
		file.lock_exclusive()?;

		let mmap = unsafe { Arc::new(Mmap::map(&file)?) };

		let mut db = DBInner{
			data: mmap,
			freelist: Freelist::new(),

			file: Mutex::new(file),
			mmap_lock: Mutex::new(()),
			open_ro_txs: Mutex::new(Vec::new()),

			pagesize,
		};

		let meta = db.meta();
		
		let free_pages = Page::from_buf(&db.data, meta.freelist_page, pagesize as usize).freelist();

		if free_pages.len() > 0 {
			db.freelist.init(free_pages);
		}

		Ok(db)
	}

	pub (crate) fn resize(&mut self, file: &File, new_size: u64) -> Result<()> {
		file.allocate(new_size)?;
		let _lock = self.mmap_lock.lock()?;
		let mmap = unsafe { Mmap::map(file).unwrap() };
		self.data = Arc::new(mmap);
		Ok(())
	}

	pub (crate) fn meta(&self) -> Meta {
		let meta1 = Page::from_buf(&self.data, 0, self.pagesize).meta();
		let meta2 = Page::from_buf(&self.data, 1, self.pagesize).meta();
		match (meta1.valid(), meta2.valid()) {
			(true, true) => {
				assert_eq!(meta1.pagesize as usize, self.pagesize, "Invalid pagesize from meta1 {}. Expected {}.", meta1.pagesize, self.pagesize);
				assert_eq!(meta2.pagesize as usize, self.pagesize, "Invalid pagesize from meta2 {}. Expected {}.", meta2.pagesize, self.pagesize);
				if meta1.tx_id > meta2.tx_id { meta1 } else { meta2 }
			},
			(true, false) => {
				assert_eq!(meta1.pagesize as usize, self.pagesize, "Invalid pagesize from meta1 {}. Expected {}.", meta1.pagesize, self.pagesize);
				meta1
			},
			(false, true) => {
				assert_eq!(meta2.pagesize as usize, self.pagesize, "Invalid pagesize from meta2 {}. Expected {}.", meta2.pagesize, self.pagesize);
				meta2				
			},
			(false, false) => panic!("NO VALID META PAGES"),
		}.clone()
	}

}

fn init_file(path: &Path, pagesize: usize, num_pages: usize) -> Result<File> {
	let mut file = FileOpenOptions::new()
		.create(true)
		.read(true)
		.write(true)
		.open(path)?;
	file.allocate((pagesize * num_pages) as u64)?;
	let mut buf = vec![0; pagesize * 4];
	
	let mut get_page = |index: usize| {
		unsafe {&mut *(&mut buf[index * pagesize] as *mut u8 as *mut Page)}
	};
	
	for i in 0..2 {
		let page = get_page(i);
		page.id = i;
		page.page_type = Page::TYPE_META;
		let m = page.meta_mut();
		m.meta_page = i as u8;
		m.magic = MAGIC_VALUE;
		m.version = VERSION;
		m.pagesize = pagesize as u32;
		m.freelist_page = 2;
		m.root = BucketMeta{root_page: 3, sequence: 0};
		m.num_pages = 3;
		m.hash = m.hash_self();
	}

	let p = get_page(2);
	p.id = 2;
	p.page_type = Page::TYPE_FREELIST;
	p.count = 0;

	let p = get_page(3);
	p.id = 3;
	p.page_type = Page::TYPE_LEAF;
	p.count = 0;

	file.write(&buf[..])?;
	file.flush()?;
	file.sync_all()?;
	Ok(file)
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::path::PathBuf;
	use rand::{Rng, distributions::Alphanumeric};

	fn random_file() -> PathBuf {
		loop {
			let filename: String = rand::thread_rng()
				.sample_iter(&Alphanumeric)
				.take(30)
				.collect();
			let path = std::env::temp_dir().join(filename);
			if let Err(_) =  path.metadata() {
				return path
			}
		}
	}

	#[test]
	fn test_open_options() {
		assert_ne!(getPageSize(), 500);
		let path = random_file();
		{
			let db = OpenOptions::new().pagesize(500).num_pages(100).open(path.clone()).unwrap();
			assert_eq!(db.pagesize(), 500);
		}
		{
			let metadata = path.metadata().unwrap();
			assert!(metadata.is_file());
			assert_eq!(metadata.len(), 50000);
		}
		{
			let db = OpenOptions::new().pagesize(500).num_pages(100).open(path.clone()).unwrap();
			assert_eq!(db.pagesize(), 500);
		}
	}

	#[test]
	#[should_panic]
	fn test_different_pagesizes() {
		assert_ne!(getPageSize(), 500);
		let path = random_file();
		{
			let db = OpenOptions::new().pagesize(500).num_pages(100).open(path.clone()).unwrap();
			assert_eq!(db.pagesize(), 500);
		}
		DB::open(path).unwrap();
	}
}