use std::io::Write;
use std::fs::{File, OpenOptions};
use std::sync::{Arc, Mutex, RwLock};

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

pub const ALLOC_SIZE: u64 = 8 * 1024 * 1024;

#[derive(Clone)]
pub struct DB(Arc<DBInner>);

impl DB {
	pub fn open(path: &str) -> Result<DB> {
		let db = DBInner::open(path)?;
		Ok(DB(Arc::new(db)))
	}

	pub fn tx(&self, writable: bool) -> Result<Transaction> {
		Transaction::new(&self.0, writable)
	}
}

pub (crate) struct DBInner {
	pub (crate) mmap_lock: RwLock<()>,
	pub (crate) data: Mmap,
	pub (crate) file: Mutex<File>,
	pub (crate) pagesize: usize,
	pub (crate) open_ro_txs: Mutex<Vec<u64>>,
	pub (crate) freelist: Freelist,
}

impl DBInner {

	pub (crate) fn open(path: &str) -> Result<DBInner> {
		let mut file = OpenOptions::new()
			.create(true)
			.read(true)
			.write(true)
			.open(path)?;

		file.lock_exclusive()?;

		let pagesize = getPageSize();
		if file.metadata()?.len() == 0 {
			init_file(&mut file, pagesize)?;
		}

		let mmap = unsafe { Mmap::map(&file)? };

		let mut db = DBInner{
			mmap_lock: RwLock::new(()),
			data: mmap,
			file: Mutex::new(file),
			pagesize,
			open_ro_txs: Mutex::new(Vec::new()),
			freelist: Freelist::new(),
		};

		let meta = db.meta();
		if meta.pagesize as usize != pagesize {
			db.pagesize = meta.pagesize as usize;
		}
		
		let free_pages = Page::from_buf(&db.data, meta.freelist_page, db.pagesize).freelist();

		if free_pages.len() > 0 {
			db.freelist.init(free_pages);
		}

		Ok(db)
	}

	pub (crate) fn resize(&mut self, file: &File, new_size: u64) -> Result<()> {
		file.allocate(new_size)?;
		let _lock = self.mmap_lock.write()?;
		let mmap = unsafe { Mmap::map(file).unwrap() };
		self.data = mmap;
		Ok(())
	}

	pub (crate) fn meta(&self) -> Meta {
		let meta1 = Page::from_buf(&self.data, 0, self.pagesize).meta();
		let meta2 = Page::from_buf(&self.data, 1, self.pagesize).meta();
		if meta1.tx_id > meta2.tx_id && meta1.valid() {
			return meta1.clone();
		} else if meta2.valid() {
			return meta2.clone();
		} else {
			panic!("NO VALID META PAGES");
		}
	}

}

fn init_file(file: &mut File, pagesize: usize) -> Result<()> {
	file.allocate((pagesize * 32) as u64)?;
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
	Ok(())
}
