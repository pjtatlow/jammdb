use std::{
    cell::RefCell,
    marker::PhantomData,
    ops::{Bound, RangeBounds},
    rc::Rc,
};

use crate::{
    bucket::{Bucket, InnerBucket},
    data::Data,
    freelist::TxFreelist,
    page::PageID,
    page_node::PageNodeID,
    BucketName, KVPair,
};

/// An iterator over a bucket
///
/// A cursor is created by using the [`cursor`](struct.Bucket.html#method.cursor)
/// function on a [`Bucket`]. It's primary purpose is to be an [`Iterator`] over
/// the bucket's [`Data`]. By default, a newly created cursor will start at the first
/// element in the bucket (sorted by key), but you can use the [`seek`](#method.seek) method to
/// move the cursor to a certain key / prefix before beginning to iterate.
///
/// Note that if the key you seek to exists, the cursor will begin to iterate after
/// the
///
/// # Examples
///
/// ```no_run
/// use jammdb::{DB, Data};
/// # use jammdb::Error;
///
/// # fn main() -> Result<(), Error> {
/// let db = DB::open("my.db")?;
/// let mut tx = db.tx(false)?;
/// let bucket = tx.get_bucket("my-bucket")?;
///
/// // create a cursor and use it to iterate over the entire bucket
/// for data in bucket.cursor() {
///     match data {
///         Data::Bucket(b) => println!("found a bucket with the name {:?}", b.name()),
///         Data::KeyValue(kv) => println!("found a kv pair {:?} {:?}", kv.key(), kv.value()),
///     }
/// }
///
/// let mut cursor = bucket.cursor();
/// // seek to the key "f"
/// // if it doesn't exist, it will start at the position where it should have been
/// cursor.seek("f");
/// //
/// for data in cursor {
/// }
///
/// # Ok(())
/// # }
/// ```
pub struct Cursor<'b, 'tx> {
    bucket: Rc<RefCell<InnerBucket<'tx>>>,
    freelist: Rc<RefCell<TxFreelist>>,
    writable: bool,
    stack: Vec<SearchPath>,
    next_called: bool,
    _phantom: PhantomData<&'b ()>,
}

impl<'b, 'tx> Cursor<'b, 'tx> {
    pub(crate) fn new(b: &Bucket<'b, 'tx>) -> Cursor<'b, 'tx> {
        Cursor {
            bucket: b.inner.clone(),
            freelist: b.freelist.clone(),
            writable: b.writable,
            stack: Vec::new(),
            next_called: false,
            _phantom: PhantomData,
        }
    }

    /// Moves the cursor to the given key.
    /// If the key does not exist, the cursor stops "just before"
    /// where the key _would_ be.
    ///
    /// Returns whether or not the key exists in the bucket.
    pub fn seek<T: AsRef<[u8]>>(&mut self, key: T) -> bool {
        self.next_called = false;
        let mut b = self.bucket.borrow_mut();
        if b.deleted {
            panic!("Cannot seek cursor on a deleted bucket.");
        }
        let (exists, stack) = search(key.as_ref(), b.meta.root_page, &mut b);
        self.stack = stack;
        exists
    }

    /// Returns the data at the cursor's current position.
    /// You can use this to get data after doing a [`seek`](#method.seek).
    pub fn current<'a>(&'a self) -> Option<Data<'b, 'tx>> {
        let b = self.bucket.borrow_mut();
        if b.deleted {
            panic!("Cannot get data from a deleted bucket.");
        }
        match self.stack.last() {
            Some(e) => {
                let n = b.page_node(e.id);
                n.val(e.index).map(|data| data.into())
            }
            None => None,
        }
    }

    fn seek_first(&mut self) {
        let b = self.bucket.borrow();
        if self.stack.is_empty() {
            self.stack.push(SearchPath {
                index: 0,
                id: PageNodeID::Page(b.meta.root_page),
            });
        }
        loop {
            let elem = self.stack.last().unwrap();
            let page_node = b.page_node(elem.id);
            if page_node.leaf() {
                break;
            }
            if page_node.len() == 0 {
                break;
            }
            let page_id = page_node.index_page(elem.index);

            self.stack.push(SearchPath {
                index: 0,
                id: PageNodeID::Page(page_id),
            });
        }
    }
}

// function that searches the bucket for a given key
pub(crate) fn search(
    key: &[u8],
    mut page_id: PageID,
    b: &mut InnerBucket,
) -> (bool, Vec<SearchPath>) {
    let mut stack = Vec::new();
    loop {
        let page_node = b.page_node(PageNodeID::Page(page_id));
        let id = page_node.id();
        let (index, exact) = page_node.index(key);
        let leaf = page_node.leaf();
        stack.push(SearchPath { index, id });
        if leaf {
            return (exact, stack);
        }
        let next_page_id = page_node.index_page(index);
        if next_page_id == 0 {
            return (false, stack);
        }
        b.add_page_parent(next_page_id, page_id);
        page_id = next_page_id;
    }
}

// Keeps track of the path we've taken to search a PageNode.
pub(crate) struct SearchPath {
    pub(crate) index: usize,
    pub(crate) id: PageNodeID,
}

impl<'b, 'tx> Iterator for Cursor<'b, 'tx> {
    type Item = Data<'b, 'tx>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.stack.is_empty() {
            self.seek_first();
        } else if self.next_called {
            loop {
                {
                    let b = self.bucket.borrow();
                    if b.deleted {
                        panic!("Cannot get data from a deleted bucket.");
                    }
                    let elem = self.stack.last_mut().unwrap();
                    let page_node = b.page_node(elem.id);
                    if elem.index >= (page_node.len() - 1) {
                        if self.stack.len() == 1 {
                            return None;
                        }
                        self.stack.pop();
                        continue;
                    } else {
                        elem.index += 1;
                    }
                }
                self.seek_first();
                break;
            }
        }
        self.next_called = true;
        self.current()
    }
}

/// A bounded iterator over the data in a bucket.
pub struct Range<'r, 'b, 'tx, R>
where
    R: RangeBounds<&'r [u8]>,
{
    pub(crate) c: Cursor<'b, 'tx>,
    pub(crate) bounds: R,
    pub(crate) _phantom: PhantomData<&'r ()>,
}

impl<'r, 'b, 'tx, R> Iterator for Range<'r, 'b, 'tx, R>
where
    R: RangeBounds<&'r [u8]>,
{
    type Item = Data<'b, 'tx>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.c.next_called {
            if let Bound::Included(s) = self.bounds.start_bound() {
                let exists = self.c.seek(*s);
                // if the start key is not there,
                // skip to the key after where it should be.
                if !exists {
                    if let Some(data) = self.c.current() {
                        if data.key() < *s {
                            self.c.next();
                        }
                    }
                }
            }
        }
        let next = self.c.next();
        match next {
            Some(data) => match self.bounds.end_bound() {
                Bound::Excluded(e) => {
                    if data.key() < *e {
                        Some(data)
                    } else {
                        None
                    }
                }
                Bound::Included(e) => {
                    if data.key() <= *e {
                        Some(data)
                    } else {
                        None
                    }
                }
                Bound::Unbounded => Some(data),
            },
            None => None,
        }
    }
}

/// An iterator over a bucket's sub-buckets.
pub struct Buckets<'b, 'tx, I> {
    pub(crate) i: I,
    pub(crate) bucket: Rc<RefCell<InnerBucket<'tx>>>,
    pub(crate) freelist: Rc<RefCell<TxFreelist>>,
    pub(crate) writable: bool,
    pub(crate) _phantom: PhantomData<&'b ()>,
}

impl<'b, 'tx: 'b, I> Iterator for Buckets<'b, 'tx, I>
where
    I: Iterator<Item = Data<'b, 'tx>>,
{
    type Item = (BucketName<'b, 'tx>, Bucket<'b, 'tx>);

    fn next(&mut self) -> Option<Self::Item> {
        for data in self.i.by_ref() {
            if let Data::Bucket(bucket_data) = data {
                let mut b = self.bucket.borrow_mut();
                if let Ok(r) = b.get_bucket(&bucket_data) {
                    return Some((
                        bucket_data,
                        Bucket {
                            writable: self.writable,
                            freelist: self.freelist.clone(),
                            inner: r,
                            _phantom: PhantomData,
                        },
                    ));
                } else {
                    panic!("Could not find bucket")
                }
            }
        }
        None
    }
}

pub trait ToBuckets<'b, 'tx: 'b>: Iterator<Item = Data<'b, 'tx>> + Sized {
    fn to_buckets(self) -> Buckets<'b, 'tx, Self>;
}

impl<'b, 'tx: 'b> ToBuckets<'b, 'tx> for Cursor<'b, 'tx> {
    fn to_buckets(self) -> Buckets<'b, 'tx, Self> {
        let freelist = self.freelist.clone();
        let bucket = self.bucket.clone();
        let writable = self.writable;
        Buckets {
            i: self,
            bucket,
            freelist,
            writable,
            _phantom: PhantomData,
        }
    }
}

impl<'r, 'b, 'tx: 'b, R> ToBuckets<'b, 'tx> for Range<'r, 'b, 'tx, R>
where
    R: RangeBounds<&'r [u8]>,
{
    fn to_buckets(self) -> Buckets<'b, 'tx, Self> {
        let freelist = self.c.freelist.clone();
        let bucket = self.c.bucket.clone();
        let writable = self.c.writable;
        Buckets {
            i: self,
            bucket,
            freelist,
            writable,
            _phantom: PhantomData,
        }
    }
}

/// An iterator over a bucket's key / value pairs.
pub struct KVPairs<I> {
    pub(crate) i: I,
}

impl<'b, 'tx, I> Iterator for KVPairs<I>
where
    I: Iterator<Item = Data<'b, 'tx>>,
{
    type Item = KVPair<'b, 'tx>;

    fn next(&mut self) -> Option<Self::Item> {
        for data in self.i.by_ref() {
            if let Data::KeyValue(kv) = data {
                return Some(kv);
            }
        }
        None
    }
}

pub trait ToKVPairs<'b, 'tx>: Iterator<Item = Data<'b, 'tx>> + Sized {
    fn to_kv_pairs(self) -> KVPairs<Self>;
}

impl<'b, 'tx> ToKVPairs<'b, 'tx> for Cursor<'b, 'tx> {
    fn to_kv_pairs(self) -> KVPairs<Self> {
        KVPairs { i: self }
    }
}

impl<'r, 'b, 'tx, R> ToKVPairs<'b, 'tx> for Range<'r, 'b, 'tx, R>
where
    R: RangeBounds<&'r [u8]>,
{
    fn to_kv_pairs(self) -> KVPairs<Self> {
        KVPairs { i: self }
    }
}

#[cfg(test)]
mod tests {
    use crate::{db::DB, errors::Result, testutil::RandomFile};

    #[test]
    fn test_iters() -> Result<()> {
        let random_file = RandomFile::new();
        let db = DB::open(&random_file)?;
        // Put in some intermixed key / value pairs and sub-buckets.
        {
            let tx = db.tx(true)?;
            let b = tx.create_bucket("abc")?;
            b.put("a", "1")?;
            b.create_bucket("b")?;
            b.put("c", "3")?;
            b.create_bucket("d")?;
            b.put("e", "5")?;
            b.create_bucket("f")?;
            tx.commit()?;
        }
        // Make sure we iterate over all sub-buckets
        {
            let tx = db.tx(false)?;
            let b = tx.get_bucket("abc")?;
            let mut buckets = b.buckets();
            // We should get the three sub-buckets in order
            let (data, _) = buckets.next().unwrap();
            assert_eq!(data.name(), b"b");
            let (data, _) = buckets.next().unwrap();
            assert_eq!(data.name(), b"d");
            let (data, _) = buckets.next().unwrap();
            assert_eq!(data.name(), b"f");
            // Make sure there are no more buckets
            assert!(buckets.next().is_none());
        }
        // Make sure we iterate over all kvpairs
        {
            let tx = db.tx(false)?;
            let b = tx.get_bucket("abc")?;
            let mut kvpairs = b.kv_pairs();

            // We should find the three kv pairs in order
            let data = kvpairs.next().unwrap();
            let (k, v) = data.kv();
            assert_eq!(k, b"a");
            assert_eq!(v, b"1");

            let data = kvpairs.next().unwrap();
            let (k, v) = data.kv();
            assert_eq!(k, b"c");
            assert_eq!(v, b"3");

            let data = kvpairs.next().unwrap();
            let (k, v) = data.kv();
            assert_eq!(k, b"e");
            assert_eq!(v, b"5");

            // There should be no more buckets
            assert!(kvpairs.next().is_none());
        }

        db.check()
    }

    #[test]
    #[should_panic]
    fn deleted_bucket_create_cursor() {
        let random_file = RandomFile::new();
        let db = DB::open(&random_file).unwrap();
        let tx = db.tx(true).unwrap();
        let b = tx.create_bucket("abc").unwrap();
        tx.delete_bucket("abc").unwrap();

        b.cursor();
    }

    #[test]
    #[should_panic]
    fn deleted_bucket_create_iterate() {
        let random_file = RandomFile::new();
        let db = DB::open(&random_file).unwrap();
        let tx = db.tx(true).unwrap();
        let b = tx.create_bucket("abc").unwrap();
        let mut c = b.cursor();
        tx.delete_bucket("abc").unwrap();
        c.next();
    }
}
