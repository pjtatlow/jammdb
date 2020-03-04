use std::io::Write;
use std::fs::{File, OpenOptions};
use std::sync::{Arc, Mutex, RwLock};

use fs2::FileExt;
use memmap::Mmap;
use page_size::{get as getPageSize};

use crate::page::{Page};
use crate::errors::{Result};
use crate::bucket::{BucketMeta};
use crate::transaction::Transaction;

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
	pub (crate) write_lock: Mutex<()>,
	pub (crate) pagesize: usize,
	// meta: Meta,
}

impl DBInner {

	pub fn open(path: &str) -> Result<DBInner> {
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

		let db = DBInner{
			mmap_lock: RwLock::new(()),
			data: mmap,
			file: Mutex::new(file),
			write_lock: Mutex::new(()),
			pagesize,
		};

		Ok(db)
	}

	pub fn resize(&mut self, file: &File, new_size: u64) -> Result<()> {
		println!("RESIZING TO {}", new_size);
		println!("OLD MMAP SIZE {}", self.data.len());
		file.allocate(new_size)?;
		println!("NEW SIZE IS {}", file.metadata()?.len());
		let _lock = self.mmap_lock.write()?;
		let mmap = unsafe { Mmap::map(file).unwrap() };
		self.data = mmap;
		println!("NEW MMAP SIZE {}", self.data.len());
		Ok(())
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
		m.magic = MAGIC_VALUE;
		m.version = VERSION;
		m.pagesize = pagesize as u32;
		m.freelist_page = 2;
		m.root = BucketMeta{root_page: 3, sequence: 0};
		m.num_pages = 3;
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
