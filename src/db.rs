use memmap2::Mmap;
use page_size::get as get_page_size;
use std::{
    fs::{File, OpenOptions as FileOpenOptions},
    io::Write,
    path::Path,
    sync::{Arc, Mutex, RwLock},
};

use crate::{bucket::BucketMeta, errors::Result, page::Page, tx::Tx};
use crate::{freelist::Freelist, meta::Meta};
use fs2::FileExt;

const MAGIC_VALUE: u32 = 0x00AB_CDEF;
const VERSION: u32 = 1;

// Minimum number of bytes to allocate when growing the databse
pub(crate) const MIN_ALLOC_SIZE: u64 = 8 * 1024 * 1024;

// Number of pages to allocate when creating the database
const DEFAULT_NUM_PAGES: usize = 32;

/// Options to configure how a [`DB`] is opened.
///
/// This struct acts as a builder for a [`DB`] and allows you to specify
/// the initial pagesize and number of pages you want to allocate for a new database file.
///
/// # Examples
///
/// ```no_run
/// use jammdb::{DB, OpenOptions};
/// # use jammdb::Error;
///
/// # fn main() -> Result<(), Error> {
/// let db = OpenOptions::new()
///     .pagesize(4096)
///     .num_pages(32)
///     .open("my.db")?;
///
/// // do whatever you want with the DB
/// # Ok(())
/// # }
/// ```
pub struct OpenOptions {
    pagesize: u64,
    num_pages: usize,
}

impl OpenOptions {
    /// Returns a new OpenOptions, with the default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the pagesize for the database
    ///
    /// By default, your OS's pagesize is used as the database's pagesize, but if the file is
    /// moved across systems with different page sizes, it is necessary to set the correct value.
    /// Trying to open an existing database with the incorrect page size will result in a panic.
    ///
    /// # Panics
    /// Will panic if you try to set the pagesize < 1024 bytes.
    pub fn pagesize(mut self, pagesize: u64) -> Self {
        if pagesize < 1024 {
            panic!("Pagesize must be 1024 bytes minimum");
        }
        self.pagesize = pagesize;
        self
    }

    /// Sets the number of pages to allocate for a new database file.
    ///
    /// The default `num_pages` is set to 32, so if your pagesize is 4096 bytes (4kb), then 131,072 bytes (128kb) will be allocated for the initial file.
    /// Setting `num_pages` when opening an existing database has no effect.
    ///
    /// # Panics
    /// Since a minimum of four pages are required for the database, this function will panic if you provide a value < 4.
    pub fn num_pages(mut self, num_pages: usize) -> Self {
        if num_pages < 4 {
            panic!("Must have a minimum of 4 pages");
        }
        self.num_pages = num_pages;
        self
    }

    /// Opens the database with the current options.
    ///
    /// If the file does not exist, it will initialize an empty database with a size of (`num_pages * pagesize`) bytes.
    /// If it does exist, the file is opened with both read and write permissions, and we attempt to create an
    /// [exclusive lock](https://en.wikipedia.org/wiki/File_locking) on the file. Getting the file lock will block until the lock
    /// is released to prevent you from having two processes modifying the file at the same time. This lock is not foolproof though,
    /// so it is up to the user to make sure only one process has access to the database at a time (unless it is read-only).
    ///
    /// # Errors
    ///
    /// Will return an error if there are issues creating a new file, opening an existing file, obtaining the file lock, or creating the memory map.
    ///
    /// # Panics
    ///
    /// Will panic if the pagesize the database is opened with is not the same as the pagesize it was created with.
    pub fn open<P: AsRef<Path>>(self, path: P) -> Result<DB> {
        let path: &Path = path.as_ref();
        let file = if !path.exists() {
            init_file(path, self.pagesize, self.num_pages)?
        } else {
            FileOpenOptions::new().read(true).write(true).open(path)?
        };

        let db = DBInner::open(file, self.pagesize)?;
        Ok(DB {
            inner: Arc::new(db),
        })
    }
}

impl Default for OpenOptions {
    fn default() -> Self {
        let pagesize = get_page_size() as u64;
        if pagesize < 1024 {
            panic!("Pagesize must be 1024 bytes minimum");
        }
        OpenOptions {
            pagesize,
            num_pages: DEFAULT_NUM_PAGES,
        }
    }
}

/// A database
///
/// A DB can created from an [`OpenOptions`] builder, or by calling [`open`](#method.open).
/// From a DB, you can create a [`Tx`] to access the data in the database.
/// If you want to use the database across threads, so you can `clone` the database
/// to have concurrent transactions (you're really just cloning an [`Arc`] so it's pretty cheap).
/// **Do not** try to open multiple transactions in the same thread, you're pretty likely to cause a deadlock.
#[derive(Clone)]
pub struct DB {
    pub(crate) inner: Arc<DBInner>,
}

impl DB {
    /// Opens a database using the default [`OpenOptions`].
    ///
    /// Same as calling `OpenOptions::new().open(path)`.
    /// Please read the documentation for [`OpenOptions::open`](struct.OpenOptions.html#method.open) for details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use jammdb::{DB};
    /// # use jammdb::Error;
    ///
    /// # fn main() -> Result<(), Error> {
    /// let db = DB::open("my.db")?;
    ///
    /// // do whatever you want with the DB
    /// # Ok(())
    /// # }
    /// ```
    pub fn open<P: AsRef<Path>>(path: P) -> Result<DB> {
        OpenOptions::new().open(path)
    }

    /// Creates a [`Tx`].
    /// This transaction is either read-only or writable depending on the `writable` parameter.
    /// Please read the docs on a [`Tx`] for more details.
    pub fn tx(&self, writable: bool) -> Result<Tx> {
        Tx::new(self, writable)
    }

    /// Returns the database's pagesize.
    pub fn pagesize(&self) -> u64 {
        self.inner.pagesize
    }

    #[doc(hidden)]
    pub fn check(&self) -> Result<()> {
        self.tx(false)?.check()
    }
}
pub(crate) struct DBInner {
    pub(crate) data: Mutex<Arc<Mmap>>,
    pub(crate) mmap_lock: RwLock<()>,
    pub(crate) freelist: Mutex<Freelist>,
    pub(crate) file: Mutex<File>,
    pub(crate) open_ro_txs: Mutex<Vec<u64>>,

    pub(crate) pagesize: u64,
}

impl DBInner {
    pub(crate) fn open(file: File, pagesize: u64) -> Result<DBInner> {
        file.lock_exclusive()?;

        let mmap = unsafe { Mmap::map(&file)? };
        let mmap = Mutex::new(Arc::new(mmap));
        let db = DBInner {
            data: mmap,
            mmap_lock: RwLock::new(()),
            freelist: Mutex::new(Freelist::new()),

            file: Mutex::new(file),
            open_ro_txs: Mutex::new(Vec::new()),

            pagesize,
        };

        {
            let meta = db.meta()?;
            let data = db.data.lock()?;
            let free_pages = Page::from_buf(&data, meta.freelist_page, pagesize).freelist();

            if !free_pages.is_empty() {
                db.freelist.lock()?.init(free_pages);
            }
        }

        Ok(db)
    }

    pub(crate) fn resize(&self, file: &File, new_size: u64) -> Result<Arc<Mmap>> {
        file.allocate(new_size)?;
        let _lock = self.mmap_lock.write()?;
        let mut data = self.data.lock()?;
        let mmap = unsafe { Mmap::map(file).unwrap() };
        *data = Arc::new(mmap);
        Ok(data.clone())
    }

    pub(crate) fn meta(&self) -> Result<Meta> {
        let data = self.data.lock()?;
        let meta1 = Page::from_buf(&data, 0, self.pagesize).meta();

        // Double check that we have the right pagesize before we read the second page.
        if meta1.valid() && meta1.pagesize != self.pagesize {
            assert_eq!(
                meta1.pagesize as u64, self.pagesize,
                "Invalid pagesize from meta1 {}. Expected {}.",
                meta1.pagesize, self.pagesize
            );
        }

        let meta2 = Page::from_buf(&data, 1, self.pagesize).meta();
        let meta = match (meta1.valid(), meta2.valid()) {
            (true, true) => {
                assert_eq!(
                    meta1.pagesize as u64, self.pagesize,
                    "Invalid pagesize from meta1 {}. Expected {}.",
                    meta1.pagesize, self.pagesize
                );
                assert_eq!(
                    meta2.pagesize as u64, self.pagesize,
                    "Invalid pagesize from meta2 {}. Expected {}.",
                    meta2.pagesize, self.pagesize
                );
                if meta1.tx_id > meta2.tx_id {
                    meta1
                } else {
                    meta2
                }
            }
            (true, false) => {
                assert_eq!(
                    meta1.pagesize as u64, self.pagesize,
                    "Invalid pagesize from meta1 {}. Expected {}.",
                    meta1.pagesize, self.pagesize
                );
                meta1
            }
            (false, true) => {
                assert_eq!(
                    meta2.pagesize as u64, self.pagesize,
                    "Invalid pagesize from meta2 {}. Expected {}.",
                    meta2.pagesize, self.pagesize
                );
                meta2
            }
            (false, false) => panic!("NO VALID META PAGES"),
        };

        Ok(meta.clone())
    }
}

fn init_file(path: &Path, pagesize: u64, num_pages: usize) -> Result<File> {
    let mut file = FileOpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(path)?;
    file.allocate(pagesize * (num_pages as u64))?;
    let mut buf = vec![0; (pagesize * 4) as usize];
    let mut get_page = |index: u64| {
        #[allow(clippy::cast_ptr_alignment)]
        unsafe {
            &mut *(&mut buf[(index * pagesize) as usize] as *mut u8 as *mut Page)
        }
    };
    for i in 0..2 {
        let page = get_page(i);
        page.id = i;
        page.page_type = Page::TYPE_META;
        let m = page.meta_mut();
        m.meta_page = i as u32;
        m.magic = MAGIC_VALUE;
        m.version = VERSION;
        m.pagesize = pagesize;
        m.freelist_page = 2;
        m.root = BucketMeta {
            root_page: 3,
            next_int: 0,
        };
        m.num_pages = 4;
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

    file.write_all(&buf[..])?;
    file.flush()?;
    file.sync_all()?;
    Ok(file)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::RandomFile;

    #[test]
    fn test_open_options() {
        assert_ne!(get_page_size(), 5000);
        let random_file = RandomFile::new();
        {
            let db = OpenOptions::new()
                .pagesize(5000)
                .num_pages(100)
                .open(&random_file)
                .unwrap();
            assert_eq!(db.pagesize(), 5000);
        }
        {
            let metadata = random_file.path.metadata().unwrap();
            assert!(metadata.is_file());
            assert_eq!(metadata.len(), 500_000);
        }
        {
            let db = OpenOptions::new()
                .pagesize(5000)
                .num_pages(100)
                .open(&random_file)
                .unwrap();
            assert_eq!(db.pagesize(), 5000);
        }
    }

    #[test]
    #[should_panic]
    fn test_open_options_min_pages() {
        OpenOptions::new().num_pages(3);
    }

    #[test]
    #[should_panic]
    fn test_open_options_min_pagesize() {
        OpenOptions::new().pagesize(1000);
    }

    #[test]
    #[should_panic]
    fn test_different_pagesizes() {
        assert_ne!(get_page_size(), 5000);
        let random_file = RandomFile::new();
        {
            let db = OpenOptions::new()
                .pagesize(5000)
                .num_pages(100)
                .open(&random_file)
                .unwrap();
            assert_eq!(db.pagesize(), 5000);
        }
        DB::open(&random_file).unwrap();
    }
}
