use std::fs::File;
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::pin::Pin;
use std::sync::{Arc, MutexGuard};

use memmap::Mmap;

use crate::bucket::Bucket;
use crate::data::SliceParts;
use crate::db::{DBInner, MIN_ALLOC_SIZE};
use crate::errors::Error;
use crate::errors::Result;
use crate::freelist::Freelist;
use crate::meta::Meta;
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
/// There are three important methods. Check out their documentation for more details:
/// 1. [`create_bucket`](#method.create_bucket) makes new buckets at the root level. Available in writable transactions.
/// 2. [`get_bucket`](#method.get_bucket) retreives buckets from the root level. Available in read-only or writable transactions.
/// 3. [`commit`](#method.commit) saves a writable transaction. Available in writable transactions.
///
/// Trying to use the methods that require writable transactions from a read-only transaction will result in an error.
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
	pub fn print_graph(&self) {
		println!("digraph G {{");
		self.inner.root.as_ref().unwrap().print();
		println!("}}");
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
	pub(crate) fn page(&self, id: usize) -> &Page {
		Page::from_buf(&self.data, id, self.db.pagesize)
	}

	pub(crate) fn get_bucket(&'a mut self, name: &[u8]) -> Result<&'a mut Bucket> {
		debug_assert!(self.root.is_some());
		if let Some(root) = self.root.as_mut() {
			return root.get_bucket(name);
		}
		panic!("");
	}

	pub(crate) fn create_bucket(&'a mut self, name: &[u8]) -> Result<&'a mut Bucket> {
		debug_assert!(self.root.is_some());
		if let Some(root) = self.root.as_mut() {
			return root.create_bucket(name);
		}
		panic!("");
	}

	pub(crate) fn copy_data(&mut self, data: &[u8]) -> SliceParts {
		let data = Vec::from(data);
		self.buffers.push(data);
		SliceParts::from_slice(&self.buffers.last().unwrap()[..])
	}

	pub(crate) fn free(&mut self, page_id: PageID, num_pages: usize) {
		for id in page_id..(page_id + num_pages) {
			self.freelist.free(self.meta.tx_id, id);
		}
	}

	pub(crate) fn allocate(&mut self, bytes: usize) -> (PageID, usize) {
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
			let mut page = unsafe { &mut *(&mut buf[0] as *mut u8 as *mut Page) };
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
				}
			};
			open_txs.remove(index);
		}
	}
}
