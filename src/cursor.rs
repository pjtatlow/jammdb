use std::{cell::RefCell, marker::PhantomData, rc::Rc};

use crate::{
    bucket::{Bucket, InnerBucket},
    data::Data,
    freelist::TxFreelist,
    page::PageID,
    page_node::PageNodeID,
    BucketData, KVPair,
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
        let (exists, stack) = search(key.as_ref(), b.meta.root_page, &mut b);
        self.stack = stack;
        exists
    }

    /// Returns the data at the cursor's current position.
    /// You can use this to get data after doing a [`seek`](#method.seek).
    pub fn current<'a>(&'a self) -> Option<Data<'tx>> {
        match self.stack.last() {
            Some(e) => {
                let b = self.bucket.borrow_mut();
                let n = b.page_node(e.id);
                n.val(e.index)
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
    type Item = Data<'tx>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.stack.is_empty() {
            self.seek_first();
        } else if self.next_called {
            loop {
                {
                    let b = self.bucket.borrow();
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

/// An iterator over a bucket's sub-buckets.
pub struct Buckets<'b, 'tx> {
    pub(crate) c: Cursor<'b, 'tx>,
}

impl<'b, 'tx: 'b> Iterator for Buckets<'b, 'tx> {
    type Item = (BucketData<'b>, Bucket<'b, 'tx>);

    fn next(&mut self) -> Option<Self::Item> {
        for data in self.c.by_ref() {
            if let Data::Bucket(bucket_data) = data {
                let mut b = self.c.bucket.borrow_mut();
                if let Ok(r) = b.get_bucket(bucket_data.name()) {
                    return Some((
                        bucket_data,
                        Bucket {
                            writable: self.c.writable,
                            freelist: self.c.freelist.clone(),
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

/// An iterator over a bucket's key / value pairs.
pub struct KVPairs<'b, 'tx> {
    pub(crate) c: Cursor<'b, 'tx>,
}

impl<'b, 'tx> Iterator for KVPairs<'b, 'tx> {
    type Item = KVPair<'tx>;

    fn next(&mut self) -> Option<Self::Item> {
        for data in self.c.by_ref() {
            if let Data::KeyValue(kv) = data {
                return Some(kv);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use crate::db::DB;
    use crate::errors::Result;
    use crate::testutil::RandomFile;

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
}
