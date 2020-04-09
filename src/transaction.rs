use std::fs::File;
use std::io::{Seek, SeekFrom, Write};
use std::pin::Pin;
use std::sync::{Arc, MutexGuard};

use memmap::Mmap;

use crate::bucket::Bucket;
use crate::data::{BucketData, SliceParts};
use crate::db::{DBInner, MIN_ALLOC_SIZE};
use crate::errors::Error;
use crate::errors::Result;
use crate::freelist::Freelist;
use crate::meta::Meta;
use crate::node::Node;
use crate::page::{Page, PageID};
use crate::ptr::Ptr;

/// An isolated view of the database
///
/// Transactions are how you can interact with the database.
/// They are created from a [`DB`](struct.DB.html),
/// and can be read-only or writable<sup>1</sup> depending on the paramater you pass into the [`tx`](struct.DB.html#method.tx) method.
/// Transactions are completely isolated from each other, so a read-only transaction can expect the data to stay exactly the same for the life
/// of the transaction, regardless of how many changes are made in other transactions<sup>2</sup>.
///
/// There are four important methods. Check out their documentation for more details:
/// 1. [`get_bucket`](#method.get_bucket) retreives buckets from the root level. Available in read-only or writable transactions.
/// 2. [`create_bucket`](#method.create_bucket) makes new buckets at the root level. Available in writable transactions only.
/// 3. [`detete_bucket`](#method.delete_bucket) deletes a bucket (including all nested buckets) from the database. Available in writable transactions only.
/// 4. [`commit`](#method.commit) saves a writable transaction. Available in writable transactions.
///
/// Trying to use the methods that require writable transactions from a read-only transaction will result in an error. If you make edits in a writable transaction,
/// and you want to save them, you must call the [`commit`](#method.commit) method, otherwise when the transaction is dropped all your changes will be lost.
///
/// # Examples
///
/// ```no_run
/// use jammdb::{DB, Data};
/// # use jammdb::Error;
///
/// # fn main() -> Result<(), Error> {
/// # let mut db = DB::open("my.db")?;
/// # // note that only one tx can be opened from each copy of the DB, so we clone it here for the example
/// # let mut db2 = db.clone();
/// // create a read-only transaction
/// let mut tx1 = db.tx(false)?;
///
/// # // hide the clone for this example
/// # let mut db = db2;
/// // create a writable transcation
/// let mut tx2 = db.tx(true)?;
///
/// // create a new bucket in the writable transaction
/// tx2.create_bucket("new-bucket")?;
///
/// // the read-only transaction will not be able to see the new bucket
/// assert!(tx1.get_bucket("new-bucket").is_err());
///
/// // get a view of an existing bucket from both transactions
/// let mut b1 = tx1.get_bucket("existing-bucket")?;
/// let mut b2 = tx2.get_bucket("existing-bucket")?;
///
/// // make an edit to the bucket
/// b2.put("new-key", "new-value")?;
///
/// // the read-only transaction will not have this new key
/// assert_eq!(b1.get("new-key"), None);
/// // but it will be able to see data that already existed!
/// assert!(b1.get("existing-key").is_some());
///
/// # Ok(())
/// # }
/// ```
///
///
/// <sup>1</sup> There can only be a single writeable transaction at a time, so trying to open
/// two writable transactions on the same thread will deadlock.
///
/// <sup>2</sup> Keep in mind that long running read-only transactions will prevent the database from
/// reclaiming old pages and your database may increase in disk size quickly if you're writing lots of data,
/// so it's a good idea to keep transactions short.
pub struct Transaction<'a> {
	inner: Pin<Box<TransactionInner>>,
	file: Option<MutexGuard<'a, File>>,
}

impl<'a> Transaction<'a> {
	pub(crate) fn new(db: &'a DBInner, writable: bool) -> Result<Transaction<'a>> {
		let file = if writable {
			Some(db.file.lock()?)
		} else {
			None
		};
		let tx = TransactionInner::new(db, writable)?;
		let mut inner = Pin::new(Box::new(tx));
		inner.init();
		Ok(Transaction { inner, file })
	}

	/// Returns a reference to the root level bucket with the given name.
	///
	/// # Errors
	///
	/// Will return a [`BucketMissing`](enum.Error.html#variant.BucketMissing) error if the bucket does not exist,
	/// or an [`IncompatibleValue`](enum.Error.html#variant.IncompatibleValue) error if the key exists but is not a bucket.
	///
	/// In a read-only transaction, you will get an error when trying to use any of the bucket's methods that modify data.
	pub fn get_bucket<T: AsRef<[u8]>>(&mut self, name: T) -> Result<&mut Bucket> {
		self.inner.get_bucket(name.as_ref())
	}

	/// Creates a new bucket with the given name and returns a reference it.
	///
	/// # Errors
	///
	/// Will return a [`BucketExists`](enum.Error.html#variant.BucketExists) error if the bucket already exists,
	/// an [`IncompatibleValue`](enum.Error.html#variant.IncompatibleValue) error if the key exists but is not a bucket,
	/// or a [`ReadOnlyTx`](enum.Error.html#variant.ReadOnlyTx) error if this is called on a read-only transaction.
	pub fn create_bucket<T: AsRef<[u8]>>(&mut self, name: T) -> Result<&mut Bucket> {
		self.inner.create_bucket(name.as_ref())
	}

	/// Deletes an existing root-level bucket with the given name
	///
	/// # Errors
	///
	/// Will return a [`BucketMissing`](enum.Error.html#variant.BucketMissing) error if the bucket does not exist,
	/// an [`IncompatibleValue`](enum.Error.html#variant.IncompatibleValue) error if the key exists but is not a bucket,
	/// or a [`ReadOnlyTx`](enum.Error.html#variant.ReadOnlyTx) error if this is called on a read-only transaction.
	pub fn delete_bucket<T: AsRef<[u8]>>(&mut self, name: T) -> Result<()> {
		self.inner.delete_bucket(name.as_ref())
	}

	/// Writes the changes made in the writeable transaction to the underlying file.
	///
	/// # Errors
	///
	/// Will return an [`IOError`](enum.Error.html#variant.IOError) error if there are any io errors while writing to disk,
	/// or a [`ReadOnlyTx`](enum.Error.html#variant.ReadOnlyTx) error if this is called on a read-only transaction.
	pub fn commit(mut self) -> Result<()> {
		if !self.inner.writable {
			return Err(Error::ReadOnlyTx);
		}
		self.inner.rebalance()?;
		self.inner.write_data(&mut self.file.as_mut().unwrap())
	}

	#[doc(hidden)]
	#[cfg_attr(tarpaulin, skip)]
	pub fn print_graph(&self) {
		println!("digraph G {{");
		self.inner.root.as_ref().unwrap().print();
		println!("}}");
	}

	pub(crate) fn check(&self) -> Result<()> {
		self.inner.check()
	}
}

pub(crate) struct TransactionInner {
	pub(crate) db: Ptr<DBInner>,
	pub(crate) meta: Meta,
	pub(crate) writable: bool,
	pub(crate) freelist: Freelist,
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

		let tx = TransactionInner {
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
	pub(crate) fn page(&self, id: u64) -> &Page {
		Page::from_buf(&self.data, id, self.db.pagesize)
	}

	fn get_bucket(&'a mut self, name: &[u8]) -> Result<&'a mut Bucket> {
		let root = self.root.as_mut().unwrap();
		root.get_bucket(name)
	}

	fn create_bucket(&'a mut self, name: &[u8]) -> Result<&'a mut Bucket> {
		let root = self.root.as_mut().unwrap();
		root.create_bucket(name)
	}

	fn delete_bucket(&mut self, name: &[u8]) -> Result<()> {
		let root = self.root.as_mut().unwrap();
		root.delete_bucket(name)
	}

	pub(crate) fn copy_data(&mut self, data: &[u8]) -> SliceParts {
		let data = Vec::from(data);
		self.buffers.push(data);
		SliceParts::from_slice(&self.buffers.last().unwrap()[..])
	}

	pub(crate) fn free(&mut self, page_id: PageID, num_pages: u64) {
		for id in page_id..(page_id + num_pages) {
			self.freelist.free(self.meta.tx_id, id);
		}
	}

	pub(crate) fn allocate(&mut self, bytes: u64) -> (PageID, u64) {
		let num_pages = if (bytes % self.db.pagesize) == 0 {
			bytes / self.db.pagesize
		} else {
			(bytes / self.db.pagesize) + 1
		};
		let page_id = match self.freelist.allocate(num_pages as usize) {
			Some(page_id) => page_id,
			None => {
				let page_id = self.meta.num_pages;
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

			let mut buf = vec![0; size as usize];

			#[allow(clippy::cast_ptr_alignment)]
			let mut page = unsafe { &mut *(&mut buf[0] as *mut u8 as *mut Page) };
			page.id = page_id;
			page.overflow = num_pages - 1;
			page.page_type = Page::TYPE_FREELIST;
			page.count = page_ids.len() as u64;
			page.freelist_mut().copy_from_slice(page_ids.as_slice());

			file.seek(SeekFrom::Start((self.db.pagesize * page_id) as u64))?;
			file.write_all(buf.as_slice())?;

			self.meta.freelist_page = page_id;
		}

		// write meta page to file
		{
			let mut buf = vec![0; self.db.pagesize as usize];

			#[allow(clippy::cast_ptr_alignment)]
			let mut page = unsafe { &mut *(&mut buf[0] as *mut u8 as *mut Page) };
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

			file.seek(SeekFrom::Start((self.db.pagesize * meta_page_id) as u64))?;
			file.write_all(buf.as_slice())?;
		}

		file.flush()?;
		file.sync_all()?;

		self.db.freelist = self.freelist.clone();
		Ok(())
	}

	fn check(&self) -> Result<()> {
		use std::collections::HashSet;
		let mut unused_pages: HashSet<PageID> = (2..self.meta.num_pages).collect();
		let mut page_stack = Vec::new();
		page_stack.push(self.meta.root.root_page);
		page_stack.push(self.meta.freelist_page);
		while !page_stack.is_empty() {
			let page_id = page_stack.pop().unwrap();
			if !unused_pages.remove(&page_id) {
				#[cfg_attr(tarpaulin, skip)]
				return Err(Error::InvalidDB(format!(
					"Page {} missing from unused_pages",
					page_id,
				)));
			}
			let page = self.page(page_id);
			for i in 0..page.overflow {
				let page_id = page_id + i + 1;
				if !unused_pages.remove(&page_id) {
					#[cfg_attr(tarpaulin, skip)]
					return Err(Error::InvalidDB(format!(
						"Overflow Page {} from missing from unused_pages",
						page_id,
					)));
				}
			}
			match page.page_type {
				Page::TYPE_BRANCH => {
					let mut last: Option<&[u8]> = None;
					for (i, b) in page.branch_elements().iter().enumerate() {
						page_stack.push(b.page);
						if i > 0 && last.unwrap() >= b.key() {
							#[cfg_attr(tarpaulin, skip)]
							return Err(Error::InvalidDB(format!(
								"Page {} contains unsorted elements",
								page_id
							)));
						}
						last = Some(b.key());
					}
				}
				Page::TYPE_LEAF => {
					let mut last: Option<&[u8]> = None;
					for (i, leaf) in page.leaf_elements().iter().enumerate() {
						match leaf.node_type {
							Node::TYPE_BUCKET => {
								let bucket_data = BucketData::new(leaf.key(), leaf.value());
								page_stack.push(bucket_data.meta().root_page);
							}
							Node::TYPE_DATA => (),
							_ =>
							{
								#[cfg_attr(tarpaulin, skip)]
								return Err(Error::InvalidDB(format!(
									"Page {} index {} has an invalid node type {}",
									page_id, i, leaf.node_type,
								)))
							}
						}
						if i > 0 && last.unwrap() >= leaf.key() {
							#[cfg_attr(tarpaulin, skip)]
							return Err(Error::InvalidDB(format!(
								"Page {} contains unsorted elements",
								page_id
							)));
						}
						last = Some(leaf.key());
					}
				}
				Page::TYPE_FREELIST => {
					if page_id != self.meta.freelist_page {
						return Err(Error::InvalidDB(format!(
							"Found Invalid Freelist Page {}",
							page_id
						)));
					}
					for page_id in page.freelist() {
						if !unused_pages.remove(&page_id) {
							#[cfg_attr(tarpaulin, skip)]
							return Err(Error::InvalidDB(format!(
								"Page {} from freelist missing from unused_pages",
								page_id,
							)));
						}
					}
				}
				_ =>
				{
					#[cfg_attr(tarpaulin, skip)]
					return Err(Error::InvalidDB(format!(
						"Invalid page type {} for page {}",
						page.page_type, page_id,
					)))
				}
			}
		}
		if !unused_pages.is_empty() {
			#[cfg_attr(tarpaulin, skip)]
			return Err(Error::InvalidDB(format!(
				"Unreachable pages {:?}",
				unused_pages,
			)));
		}
		Ok(())
	}
}

impl Drop for TransactionInner {
	fn drop(&mut self) {
		if !self.writable {
			let mut open_txs = self.db.open_ro_txs.lock().unwrap();
			let index = match open_txs.binary_search(&self.meta.tx_id) {
				Ok(i) => i,
				_ => return, // this shouldn't happen, but isn't the end of the world if it does
			};
			open_txs.remove(index);
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::db::{OpenOptions, DB};
	use crate::testutil::RandomFile;

	#[test]
	fn test_ro_txs() -> Result<()> {
		let random_file = RandomFile::new();
		let mut db = DB::open(&random_file)?;

		{
			let mut tx = db.tx(true)?;
			assert!(tx.create_bucket("abc").is_ok());
			tx.commit()?;
		}

		let mut tx = db.tx(false)?;
		assert!(tx.create_bucket("def").is_err());
		let b = tx.get_bucket("abc")?;
		assert_eq!(b.put("key", "value"), Err(Error::ReadOnlyTx));
		assert_eq!(b.delete("key"), Err(Error::ReadOnlyTx));
		assert_eq!(b.create_bucket("dev").err(), Some(Error::ReadOnlyTx));
		assert_eq!(tx.commit(), Err(Error::ReadOnlyTx));

		Ok(())
	}

	#[test]
	fn test_concurrent_txs() -> Result<()> {
		let random_file = RandomFile::new();
		let mut db = OpenOptions::new()
			.pagesize(1024)
			.num_pages(4)
			.open(&random_file)?;
		{
			// create a read-only tx
			let mut db_clone = db.clone();
			let tx = db_clone.tx(false)?;
			assert!(tx.file.is_none());
			let tx: &TransactionInner = &tx.inner;
			assert_eq!(tx.data.len(), 1024 * 4);
			assert!(tx.root.is_some());
			{
				let open_ro_txs = tx.db.open_ro_txs.lock().unwrap();
				assert_eq!(open_ro_txs.len(), 1);
				assert_eq!(open_ro_txs[0], tx.meta.tx_id);
			}
			{
				// create a writable transaction while the read-only transaction is still open
				let mut db_clone = db.clone();
				let mut tx = db_clone.tx(true)?;
				assert!(tx.file.is_some());
				assert_eq!(tx.inner.meta.tx_id, 1);
				assert_eq!(tx.inner.freelist.pages(), vec![]);
				let b = tx.create_bucket("abc")?;
				b.put("123", "456")?;
				tx.commit()?;
			}
			{
				// create a second writable transaction while the read-only transaction is still open
				let mut db_clone = db.clone();
				let mut tx = db_clone.tx(true)?;
				assert!(tx.file.is_some());
				assert_eq!(tx.inner.meta.tx_id, 2);
				assert_eq!(tx.inner.freelist.pages(), vec![2, 3]);
				let b = tx.get_bucket("abc")?;
				b.put("123", "456")?;
				tx.commit()?;
			}
			// let the read-only tx drop
		}
		{
			// make sure we can reuse the freelist
			let mut tx = db.tx(true)?;
			assert!(tx.file.is_some());
			assert_eq!(tx.inner.freelist.pages(), vec![2, 3, 4, 5, 6]);
			// allocate some pages from the freelist
			assert_eq!(tx.inner.meta.num_pages, 10);
			assert_eq!(tx.inner.allocate(1), (2, 1));
			assert_eq!(tx.inner.allocate(1), (3, 1));
			assert_eq!(tx.inner.allocate(1), (4, 1));
			assert_eq!(tx.inner.allocate(1), (5, 1));
			assert_eq!(tx.inner.allocate(1), (6, 1));
			// freelist should be empty so make sure the page is new
			assert_eq!(tx.inner.meta.num_pages, 10);
			assert_eq!(tx.inner.allocate(1), (10, 1));
			assert_eq!(tx.inner.meta.num_pages, 11);
			assert_eq!(tx.inner.freelist.pages(), vec![]);
		}
		Ok(())
	}

	#[test]
	fn test_allocate_no_freelist() -> Result<()> {
		let random_file = RandomFile::new();
		let mut db = OpenOptions::new()
			.pagesize(1024)
			.num_pages(4)
			.open(&random_file)?;
		let tx = db.tx(false)?;
		let mut tx = tx.inner;
		// make sure we have an empty freelist and only four pages
		assert_eq!(tx.freelist.pages().len(), 0);
		assert_eq!(tx.meta.num_pages, 4);
		// allocate one page worth of bytes
		assert_eq!(tx.allocate(1024), (4, 1));
		// allocate a half page worth of bytes
		assert_eq!(tx.allocate(512), (5, 1));
		// allocate ten pages worth of bytes
		assert_eq!(tx.allocate(10240), (6, 10));
		// allocate a non pagesize number of bytes
		assert_eq!(tx.allocate(1234), (16, 2));
		Ok(())
	}

	#[test]
	fn test_allocate_freelist() -> Result<()> {
		let random_file = RandomFile::new();
		let mut db = OpenOptions::new()
			.pagesize(1024)
			.num_pages(100)
			.open(&random_file)?;
		let tx = db.tx(false)?;
		let mut tx = tx.inner;

		// setup the freelist and num_pages to simulate a used database
		for page in [10_u64, 11, 13, 14, 15].iter() {
			tx.freelist.free(0, *page);
		}
		tx.freelist.release(1);
		tx.meta.num_pages = 99;

		// allocate one page worth of bytes (should come from freelist)
		assert_eq!(tx.allocate(1024), (10, 1));
		// allocate a half page worth of bytes (should come from freelist)
		assert_eq!(tx.allocate(512), (11, 1));
		// allocate 3 pages worth of bytes (should come from freelist)
		assert_eq!(tx.allocate(3000), (13, 3));
		// allocate one byte (freelist should be empty now)
		assert_eq!(tx.allocate(1), (99, 1));
		Ok(())
	}

	#[test]
	fn test_free() -> Result<()> {
		let random_file = RandomFile::new();
		let mut db = OpenOptions::new()
			.pagesize(1024)
			.num_pages(100)
			.open(&random_file)?;
		let tx = db.tx(false)?;
		let mut tx = tx.inner;

		assert_eq!(tx.meta.tx_id, 0);
		assert_eq!(tx.freelist.pages().len(), 0);
		tx.free(80, 1);
		assert_eq!(tx.freelist.pages(), vec![80]);
		tx.free(100, 6);
		assert_eq!(tx.freelist.pages(), vec![80, 100, 101, 102, 103, 104, 105]);

		Ok(())
	}

	#[test]
	fn test_copy_data() -> Result<()> {
		let random_file = RandomFile::new();
		let mut db = OpenOptions::new()
			.pagesize(1024)
			.num_pages(100)
			.open(&random_file)?;
		let tx = db.tx(false)?;
		let mut tx = tx.inner;

		let data = vec![1, 2, 3];
		let parts = tx.copy_data(&data);
		assert_eq!(parts.slice(), data.as_slice());
		let data2 = vec![4, 5, 6];
		let parts2 = tx.copy_data(&data2);
		assert_eq!(parts.slice(), data.as_slice());
		assert_eq!(parts2.slice(), data2.as_slice());
		assert_eq!(tx.buffers.len(), 2);
		assert_eq!(tx.buffers[0], data);
		assert_eq!(tx.buffers[1], data2);
		Ok(())
	}
}
