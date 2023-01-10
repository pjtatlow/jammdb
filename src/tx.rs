use std::{
    cell::RefCell,
    collections::HashSet,
    fs::File,
    io::{Seek, SeekFrom, Write},
    rc::Rc,
    sync::{MutexGuard, RwLockReadGuard},
};

use crate::{
    bucket::{Bucket, InnerBucket},
    bytes::ToBytes,
    cursor::Buckets,
    db::{DB, MIN_ALLOC_SIZE},
    errors::{Error, Result},
    freelist::TxFreelist,
    meta::Meta,
    node::Node,
    page::{Page, PageID, Pages},
    BucketData,
};

pub(crate) enum TxLock<'tx> {
    Rw(MutexGuard<'tx, File>),
    Ro(RwLockReadGuard<'tx, ()>),
}

impl<'tx> TxLock<'tx> {
    fn writable(&self) -> bool {
        match self {
            Self::Rw(_) => true,
            Self::Ro(_) => false,
        }
    }
}

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
/// # let db = DB::open("my.db")?;
/// // create a read-only transaction
/// let mut tx1 = db.tx(false)?;
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
pub struct Tx<'tx> {
    pub(crate) inner: RefCell<TxInner<'tx>>,
}

pub(crate) struct TxInner<'tx> {
    pub(crate) db: &'tx DB,
    pub(crate) lock: TxLock<'tx>,
    pub(crate) root: Rc<RefCell<InnerBucket<'tx>>>,
    pub(crate) meta: Meta,
    pub(crate) freelist: Rc<RefCell<TxFreelist>>,
    pages: Pages,
    num_freelist_pages: u64,
}

impl<'tx> Tx<'tx> {
    pub(crate) fn new(db: &'tx DB, writable: bool) -> Result<Tx<'tx>> {
        let lock = match writable {
            true => TxLock::Rw(db.inner.file.lock()?),
            false => TxLock::Ro(db.inner.mmap_lock.read()?),
        };
        let mut freelist = db.inner.freelist.lock()?.clone();
        let mut meta = db.inner.meta()?;
        debug_assert!(meta.valid());
        {
            let mut open_ro_txs = db.inner.open_ro_txs.lock().unwrap();
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
        let freelist = Rc::new(RefCell::new(TxFreelist::new(meta.clone(), freelist)));

        let data = db.inner.data.lock()?.clone();
        let pages = Pages::new(data, db.inner.pagesize);
        let num_freelist_pages = pages.page(meta.freelist_page).overflow + 1;
        let root = InnerBucket::from_meta(meta.root, pages.clone());
        let root = Rc::new(RefCell::new(root));
        let inner = TxInner {
            db,
            lock,
            root,
            meta,
            freelist,
            num_freelist_pages,
            pages,
        };
        Ok(Tx {
            inner: RefCell::new(inner),
        })
    }

    pub(crate) fn writable(&self) -> bool {
        self.inner.borrow().lock.writable()
    }

    /// Returns a reference to the root level bucket with the given name.
    ///
    /// # Errors
    ///
    /// Will return a [`BucketMissing`](enum.Error.html#variant.BucketMissing) error if the bucket does not exist,
    /// or an [`IncompatibleValue`](enum.Error.html#variant.IncompatibleValue) error if the key exists but is not a bucket.
    ///
    /// In a read-only transaction, you will get an error when trying to use any of the bucket's methods that modify data.    
    pub fn get_bucket<'b, T: ToBytes<'tx>>(&'b self, name: T) -> Result<Bucket<'tx>> {
        let tx = self.inner.borrow();
        let mut root = tx.root.borrow_mut();
        let inner = root.get_bucket(name)?;
        Ok(Bucket {
            inner,
            freelist: tx.freelist.clone(),
            writable: tx.lock.writable(),
        })
    }

    /// Creates a new bucket with the given name and returns a reference it.
    ///
    /// # Errors
    ///
    /// Will return a [`BucketExists`](enum.Error.html#variant.BucketExists) error if the bucket already exists,
    /// an [`IncompatibleValue`](enum.Error.html#variant.IncompatibleValue) error if the key exists but is not a bucket,
    /// or a [`ReadOnlyTx`](enum.Error.html#variant.ReadOnlyTx) error if this is called on a read-only transaction.
    pub fn create_bucket<'b, T: ToBytes<'tx>>(&'b self, name: T) -> Result<Bucket<'tx>> {
        let tx = self.inner.borrow();
        if !tx.lock.writable() {
            return Err(Error::ReadOnlyTx);
        }
        let mut root = tx.root.borrow_mut();
        let inner = root.create_bucket(name)?;
        Ok(Bucket {
            inner,
            freelist: tx.freelist.clone(),
            writable: true,
        })
    }

    /// Creates an existing root-level bucket with the given name if it does not already exist.
    /// Gets the existing bucket if it does exist.
    ///
    /// # Errors
    ///
    /// Will return an [`IncompatibleValue`](enum.Error.html#variant.IncompatibleValue) error if the key exists but is not a bucket,
    /// or a [`ReadOnlyTx`](enum.Error.html#variant.ReadOnlyTx) error if this is called on a read-only transaction.
    pub fn get_or_create_bucket<'b, T: ToBytes<'tx>>(&'b self, name: T) -> Result<Bucket<'tx>> {
        let tx = self.inner.borrow();
        if !tx.lock.writable() {
            return Err(Error::ReadOnlyTx);
        }
        let mut root = tx.root.borrow_mut();
        let inner = root.get_or_create_bucket(name)?;
        Ok(Bucket {
            inner,
            freelist: tx.freelist.clone(),
            writable: true,
        })
    }

    /// Deletes an existing root-level bucket with the given name
    ///
    /// # Errors
    ///
    /// Will return a [`BucketMissing`](enum.Error.html#variant.BucketMissing) error if the bucket does not exist,
    /// an [`IncompatibleValue`](enum.Error.html#variant.IncompatibleValue) error if the key exists but is not a bucket,
    /// or a [`ReadOnlyTx`](enum.Error.html#variant.ReadOnlyTx) error if this is called on a read-only transaction.
    pub fn delete_bucket<T: ToBytes<'tx>>(&self, key: T) -> Result<()> {
        let tx = self.inner.borrow();
        if !tx.lock.writable() {
            return Err(Error::ReadOnlyTx);
        }
        let freelist = tx.freelist.clone();
        let mut freelist = freelist.borrow_mut();
        let mut root = tx.root.borrow_mut();
        root.delete_bucket(key, &mut freelist)
    }

    /// Iterator over the root level buckets
    pub fn buckets<'b>(&'b self) -> Buckets<'tx> {
        let tx = self.inner.borrow();
        let bucket = Bucket {
            inner: tx.root.clone(),
            freelist: tx.freelist.clone(),
            writable: tx.lock.writable(),
        };
        Buckets { c: bucket.cursor() }
    }

    /// Writes the changes made in the writeable transaction to the underlying file.
    ///
    /// # Errors
    ///
    /// Will return an [`IOError`](enum.Error.html#variant.IOError) error if there are any io errors while writing to disk,
    /// or a [`ReadOnlyTx`](enum.Error.html#variant.ReadOnlyTx) error if this is called on a read-only transaction.
    pub fn commit(self) -> Result<()> {
        if !self.writable() {
            return Err(Error::ReadOnlyTx);
        }
        let mut tx = self.inner.borrow_mut();
        let freelist = tx.freelist.clone();
        let mut freelist = freelist.borrow_mut();
        {
            let mut root = tx.root.borrow_mut();
            root.rebalance(&mut freelist)?;
        }
        tx.write_data(&mut freelist)
    }

    pub(crate) fn check(&self) -> Result<()> {
        self.inner.borrow().check()
    }
}

impl<'tx> TxInner<'tx> {
    fn write_data(&mut self, freelist: &mut TxFreelist) -> Result<()> {
        if let TxLock::Rw(file) = &mut self.lock {
            // Allocate space for the freelist
            freelist.free(self.meta.freelist_page, self.num_freelist_pages);
            let freelist_size = freelist.inner.size();
            let freelist_allocation = freelist.allocate(freelist_size);

            // Update our num_pages from the freelist now that we've allocated everything
            self.meta.num_pages = freelist.meta.num_pages;

            let required_size = (self.meta.num_pages * self.db.inner.pagesize) as u64;
            let current_size = file.metadata()?.len();
            if current_size < required_size {
                let size_diff = required_size - current_size;
                let alloc_size = ((size_diff / MIN_ALLOC_SIZE) + 1) * MIN_ALLOC_SIZE;
                self.db.inner.resize(file, current_size + alloc_size)?;
            }

            // write the data to the file
            {
                let root = self.root.borrow_mut();
                root.write(&mut *file)?;
                self.meta.root = root.meta;
            }

            // write freelist to file
            {
                let (page_id, num_pages) = freelist_allocation;
                let page_ids = freelist.inner.pages();

                let mut buf = vec![0; freelist_size as usize];

                #[allow(clippy::cast_ptr_alignment)]
                let mut page = unsafe { &mut *(&mut buf[0] as *mut u8 as *mut Page) };
                page.id = page_id;
                page.overflow = num_pages - 1;
                page.page_type = Page::TYPE_FREELIST;
                page.count = page_ids.len() as u64;
                page.freelist_mut().copy_from_slice(page_ids.as_slice());

                file.seek(SeekFrom::Start((self.db.inner.pagesize * page_id) as u64))?;
                file.write_all(buf.as_slice())?;

                self.meta.freelist_page = page_id;
            }

            // write meta page to file
            {
                let mut buf = vec![0; self.db.inner.pagesize as usize];

                #[allow(clippy::cast_ptr_alignment)]
                let mut page = unsafe { &mut *(&mut buf[0] as *mut u8 as *mut Page) };
                let meta_page_id = u64::from(self.meta.meta_page == 0);
                page.id = meta_page_id;
                page.page_type = Page::TYPE_META;
                let m = page.meta_mut();
                m.meta_page = meta_page_id as u32;
                m.magic = self.meta.magic;
                m.version = self.meta.version;
                m.pagesize = self.meta.pagesize;
                m.root = self.meta.root;
                m.num_pages = self.meta.num_pages;
                m.freelist_page = self.meta.freelist_page;
                m.tx_id = self.meta.tx_id;
                m.hash = m.hash_self();

                file.seek(SeekFrom::Start(
                    (self.db.inner.pagesize * meta_page_id) as u64,
                ))?;
                file.write_all(buf.as_slice())?;
            }

            file.flush()?;
            file.sync_all()?;

            let mut lock = self.db.inner.freelist.lock()?;
            *lock = freelist.inner.clone();
            Ok(())
        } else {
            unreachable!()
        }
    }

    fn check(&self) -> Result<()> {
        let mut unused_pages: HashSet<PageID> = (2..self.meta.num_pages).collect();
        let mut page_stack = Vec::new();
        page_stack.push(self.meta.root.root_page);
        page_stack.push(self.meta.freelist_page);
        while !page_stack.is_empty() {
            let page_id = page_stack.pop().unwrap();
            // Make sure this page hasn't already been used
            if !unused_pages.remove(&page_id) {
                return Err(Error::InvalidDB(format!(
                    "Page {} missing from unused_pages",
                    page_id,
                )));
            }
            let page = self.pages.page(page_id);
            // Make sure none of the overflow pages have been used
            for i in 0..page.overflow {
                let page_id = page_id + i + 1;
                if !unused_pages.remove(&page_id) {
                    return Err(Error::InvalidDB(format!(
                        "Overflow Page {} from missing from unused_pages",
                        page_id,
                    )));
                }
            }
            // Check the page type and explore all possible pages
            match page.page_type {
                Page::TYPE_BRANCH => {
                    let mut last: Option<&[u8]> = None;
                    for b in page.branch_elements().iter() {
                        // Make sure we visit every branch page
                        page_stack.push(b.page);
                        // and that the keys are in order
                        if let Some(last) = last {
                            if last >= b.key() {
                                return Err(Error::InvalidDB(format!(
                                    "Page {} contains unsorted elements",
                                    page_id
                                )));
                            }
                        }
                        last = Some(b.key());
                    }
                }
                Page::TYPE_LEAF => {
                    let mut last: Option<&[u8]> = None;
                    for (i, leaf) in page.leaf_elements().iter().enumerate() {
                        match leaf.node_type {
                            Node::TYPE_BUCKET => {
                                let bucket_data: BucketData = leaf.into();
                                // Push all nested bucket pages onto the queue for exploration
                                page_stack.push(bucket_data.meta().root_page);
                            }
                            // Ignore data nodes since they don't point to more pages
                            Node::TYPE_DATA => (),
                            // If somehow it isn't a bucket or data, that's really bad...
                            _ => {
                                return Err(Error::InvalidDB(format!(
                                    "Page {} index {} has an invalid leaf node type {}",
                                    page_id, i, leaf.node_type,
                                )))
                            }
                        }
                        // Make sure all leaf elements are in order
                        if let Some(last) = last {
                            if last >= leaf.key() {
                                return Err(Error::InvalidDB(format!(
                                    "Page {} contains unsorted elements",
                                    page_id
                                )));
                            }
                        }
                        last = Some(leaf.key());
                    }
                }
                Page::TYPE_FREELIST => {
                    // Make sure our metadata is pointing at the correct freelist page
                    // and we didn't somehow find our way to another one.
                    if page_id != self.meta.freelist_page {
                        return Err(Error::InvalidDB(format!(
                            "Found Invalid Freelist Page {}",
                            page_id
                        )));
                    }
                    // "visit" all freelist pages (we don't actually care what data is in these pages)
                    for page_id in page.freelist() {
                        if !unused_pages.remove(page_id) {
                            return Err(Error::InvalidDB(format!(
                                "Page {} from freelist missing from unused_pages",
                                page_id,
                            )));
                        }
                    }
                }
                // There are no other valid page types, so getting here is really bad 😅
                _ => {
                    return Err(Error::InvalidDB(format!(
                        "Invalid page type {} for page {}",
                        page.page_type, page_id,
                    )))
                }
            }
        }

        // Once we've explored all of the pages we can reach from the root bucket and freelist,
        // If there are any pages left then we have an invalid database.
        if !unused_pages.is_empty() {
            return Err(Error::InvalidDB(format!(
                "Unreachable pages {:?}",
                unused_pages,
            )));
        }
        Ok(())
    }
}

impl<'tx> Drop for TxInner<'tx> {
    fn drop(&mut self) {
        if !self.lock.writable() {
            let mut open_txs = self.db.inner.open_ro_txs.lock().unwrap();
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
        let db = DB::open(&random_file)?;

        {
            let tx = db.tx(true)?;
            assert!(tx.create_bucket("abc").is_ok());
            tx.commit()?;
        }

        let tx = db.tx(false)?;
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
        let db = OpenOptions::new()
            .pagesize(1024)
            // make sure we have plenty of pages so we don't have to resize while the read-only tx is open
            .num_pages(10)
            .open(&random_file)?;
        {
            // create a read-only tx
            let tx = db.tx(false)?;
            assert!(!tx.writable());
            let tx = tx.inner.borrow_mut();
            assert_eq!(tx.pages.data.len(), 1024 * 10);
            assert!(!tx.lock.writable());
            {
                let open_ro_txs = tx.db.inner.open_ro_txs.lock().unwrap();
                assert_eq!(open_ro_txs.len(), 1);
                assert_eq!(open_ro_txs[0], tx.meta.tx_id);
            }
            {
                // create a writable transaction while the read-only transaction is still open
                let tx = db.tx(true)?;
                assert!(tx.writable());
                {
                    {
                        let inner = tx.inner.borrow_mut();
                        assert_eq!(inner.meta.tx_id, 1);
                        let freelist = inner.freelist.borrow();
                        assert_eq!(freelist.inner.pages(), vec![]);
                    }
                    let b = tx.create_bucket("abc")?;
                    b.put("123", "456")?;
                }
                tx.commit()?;
            }
            {
                // create a second writable transaction while the read-only transaction is still open
                let tx = db.tx(true)?;
                assert!(tx.writable());
                {
                    {
                        let inner = tx.inner.borrow_mut();
                        let freelist = inner.freelist.borrow();
                        assert_eq!(inner.meta.tx_id, 2);
                        assert_eq!(freelist.inner.pages(), vec![2, 3]);
                    }
                    let b = tx.get_bucket("abc")?;
                    b.put("123", "456")?;
                }
                tx.commit()?;
            }
            // let the read-only tx drop
        }
        {
            // make sure we can reuse the freelist
            let tx = db.tx(true)?;
            assert!(tx.writable());
            let inner = tx.inner.borrow_mut();
            let mut freelist = inner.freelist.borrow_mut();
            assert_eq!(freelist.inner.pages(), vec![2, 3, 4, 5, 6]);
            // allocate some pages from the freelist
            assert_eq!(freelist.meta.num_pages, 10);
            assert_eq!(freelist.allocate(1), (2, 1));
            assert_eq!(freelist.allocate(1), (3, 1));
            assert_eq!(freelist.allocate(1), (4, 1));
            assert_eq!(freelist.allocate(1), (5, 1));
            assert_eq!(freelist.allocate(1), (6, 1));
            // freelist should be empty so make sure the page is new
            assert_eq!(freelist.meta.num_pages, 10);
            assert_eq!(freelist.allocate(1), (10, 1));
            assert_eq!(freelist.meta.num_pages, 11);
            assert_eq!(freelist.inner.pages(), vec![]);
        }
        Ok(())
    }
}
