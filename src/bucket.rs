use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::File;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;

use crate::cursor::{Buckets, Cursor, KVPairs, PageNode, PageNodeID};
use crate::data::{BucketData, Data, KVPair, Ref};
use crate::errors::{Error, Result};
use crate::node::{Branch, Node, NodeData, NodeID};
use crate::page::{Page, PageID};
use crate::ptr::Ptr;
use crate::transaction::TransactionInner;

/// A collection of data
///
/// Buckets contain a collection of data, sorted by key.
/// The data can either be key / value pairs, or nested buckets.
/// You can use buckets to [`get`](#method.get) and [`put`](#method.put) data,
/// as well as [`get`](#method.get_bucket) and [`create`](#method.create_bucket)
/// nested buckets.
///
/// You can use a [`Cursor`] to iterate over all the data in a bucket.
///
/// Buckets have an inner auto-incremented counter that keeps track
/// of how many unique keys have been inserted into the bucket.
/// You can access that using the [`next_int()`](#method.next_int) function.
///
/// # Examples
///
/// ```no_run
/// use jammdb::{DB, Data};
/// # use jammdb::Error;
///
/// # fn main() -> Result<(), Error> {
/// let db = DB::open("my.db")?;
/// let mut tx = db.tx(true)?;
///
/// // create a root-level bucket
/// let bucket = tx.create_bucket("my-bucket")?;
///
/// // create nested bucket
/// bucket.create_bucket("nested-bucket")?;
///
/// // insert a key / value pair (using &str)
/// bucket.put("key", "value");
///
/// // insert a key / value pair (using [u8])
/// bucket.put([1,2,3], [4,5,6]);
///
/// for data in bucket.cursor() {
///     match &*data {
///         Data::Bucket(b) => println!("found a bucket with the name {:?}", b.name()),
///         Data::KeyValue(kv) => println!("found a kv pair {:?} {:?}", kv.key(), kv.value()),
///     }
/// }
///
/// println!("Bucket next_int {:?}", bucket.next_int());
/// # Ok(())
/// # }
/// ```
pub struct Bucket {
    inner: RefCell<BucketInner>,
}

impl Bucket {
    pub(crate) fn root(tx: Ptr<TransactionInner>) -> Bucket {
        let meta = tx.meta.root;
        Bucket::new(BucketInner {
            tx,
            meta,
            root: PageNodeID::Page(meta.root_page),
            dirty: false,
            buckets: HashMap::new(),
            nodes: Vec::new(),
            page_node_ids: HashMap::new(),
            page_parents: HashMap::new(),
        })
    }

    pub(crate) fn new(inner: BucketInner) -> Bucket {
        Bucket {
            inner: RefCell::new(inner),
        }
    }

    /// Adds to or replaces key / value data in the bucket.
    /// Returns an error if the key currently exists but is a bucket instead of a key / value pair.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use jammdb::{DB};
    /// # use jammdb::Error;
    ///
    /// # fn main() -> Result<(), Error> {
    /// let db = DB::open("my.db")?;
    /// let mut tx = db.tx(true)?;
    ///
    /// // create a root-level bucket
    /// let bucket = tx.create_bucket("my-bucket")?;
    ///
    /// // insert data
    /// bucket.put("123", "456")?;
    ///
    /// // update data
    /// bucket.put("123", "789")?;
    ///
    /// bucket.create_bucket("nested-bucket")?;
    ///
    /// assert!(bucket.put("nested-bucket", "data").is_err());
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub fn put<T: AsRef<[u8]>, S: AsRef<[u8]>>(
        &self,
        key: T,
        value: S,
    ) -> Result<Option<Ref<KVPair>>> {
        Ok(self.inner.borrow_mut().put(key, value)?.map(Ref::new))
    }

    /// Get a cursor to iterate over the bucket.
    ///
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
    ///
    /// let bucket = tx.get_bucket("my-bucket")?;
    ///
    /// for data in bucket.cursor() {
    ///     match &*data {
    ///         Data::Bucket(b) => println!("found a bucket with the name {:?}", b.name()),
    ///         Data::KeyValue(kv) => println!("found a kv pair {:?} {:?}", kv.key(), kv.value()),
    ///     }
    /// }
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub fn cursor(&self) -> Cursor {
        Cursor::new(Ptr::new(&self.inner.borrow()))
    }

    /// Gets an already created bucket.
    ///
    /// Returns an error if
    /// 1. the given key does not exist
    /// 2. the key is for key / value data, not a bucket
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use jammdb::{DB};
    /// # use jammdb::Error;
    ///
    /// # fn main() -> Result<(), Error> {
    /// let db = DB::open("my.db")?;
    /// let mut tx = db.tx(false)?;
    ///
    /// // get a root-level bucket
    /// let bucket = tx.get_bucket("my-bucket")?;
    ///
    /// // get nested bucket
    /// let mut sub_bucket = bucket.get_bucket("nested-bucket")?;
    ///
    /// // get nested bucket
    /// let sub_sub_bucket = sub_bucket.get_bucket("double-nested-bucket")?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub fn get_bucket<T: AsRef<[u8]>>(&self, name: T) -> Result<BucketRef> {
        let mut inner = self.inner.borrow_mut();
        let r = inner.bucket_getter(name.as_ref(), false, false)?;
        Ok(BucketRef::new(unsafe { &*r.bucket }))
    }

    /// Deletes a key / value pair from the bucket
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use jammdb::{DB};
    /// # use jammdb::Error;
    ///
    /// # fn main() -> Result<(), Error> {
    /// let db = DB::open("my.db")?;
    /// let mut tx = db.tx(false)?;
    ///
    /// let bucket = tx.get_bucket("my-bucket")?;
    /// // check if data is there
    /// assert!(bucket.get_kv("some-key").is_some());
    /// // delete the key / value pair
    /// bucket.delete("some-key")?;
    /// // data should no longer exist
    /// assert!(bucket.get_kv("some-key").is_none());
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub fn delete<T: AsRef<[u8]>>(&self, key: T) -> Result<Ref<KVPair>> {
        Ok(Ref::new(self.inner.borrow_mut().delete(key)?))
    }

    /// Creates a new bucket.
    ///
    /// Returns an error if
    /// 1. the given key already exists
    /// 2. It is in a read-only transaction
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use jammdb::{DB};
    /// # use jammdb::Error;
    ///
    /// # fn main() -> Result<(), Error> {
    /// let db = DB::open("my.db")?;
    /// let mut tx = db.tx(true)?;
    ///
    /// // create a root-level bucket
    /// let bucket = tx.create_bucket("my-bucket")?;
    ///
    /// // create nested bucket
    /// let mut sub_bucket = bucket.create_bucket("nested-bucket")?;
    ///
    /// // create nested bucket
    /// let mut sub_sub_bucket = sub_bucket.create_bucket("double-nested-bucket")?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub fn create_bucket<T: AsRef<[u8]>>(&self, name: T) -> Result<BucketRef> {
        let mut inner = self.inner.borrow_mut();
        let r = inner.create_bucket(name)?;
        Ok(BucketRef::new(unsafe { &*r.bucket }))
    }

    /// Creates a new bucket if it doesn't exist
    ///
    /// Returns an error if
    /// 1. It is in a read-only transaction
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use jammdb::{DB};
    /// # use jammdb::Error;
    ///
    /// # fn main() -> Result<(), Error> {
    /// let db = DB::open("my.db")?;
    /// {
    ///     let mut tx = db.tx(true)?;
    ///     // create a root-level bucket
    ///     let bucket = tx.get_or_create_bucket("my-bucket")?;
    ///     tx.commit()?;
    /// }
    /// {
    ///     let mut tx = db.tx(true)?;
    ///     // get the existing a root-level bucket
    ///     let bucket = tx.get_or_create_bucket("my-bucket")?;
    /// }
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub fn get_or_create_bucket<T: AsRef<[u8]>>(&self, name: T) -> Result<BucketRef> {
        let mut inner = self.inner.borrow_mut();
        let r = inner.get_or_create_bucket(name)?;
        Ok(BucketRef::new(unsafe { &*r.bucket }))
    }

    /// Deletes an bucket.
    ///
    /// Returns an error if
    /// 1. the given key does not exist
    /// 2. the key is for key / value data, not a bucket
    /// 3. It is in a read-only transaction
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use jammdb::{DB};
    /// # use jammdb::Error;
    ///
    /// # fn main() -> Result<(), Error> {
    /// let db = DB::open("my.db")?;
    /// let mut tx = db.tx(true)?;
    ///
    /// // get a root-level bucket
    /// let bucket = tx.get_bucket("my-bucket")?;
    ///
    /// // delete nested bucket
    /// bucket.delete_bucket("nested-bucket")?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub fn delete_bucket<T: AsRef<[u8]>>(&self, name: T) -> Result<()> {
        self.inner.borrow_mut().delete_bucket(name)
    }

    /// Returns the next integer for the bucket.
    /// The integer is automatically incremented each time a new key is added to the bucket.
    /// You can it as a unique key for the bucket, since it will increment each time you add something new.
    /// It will not increment if you [`put`](#method.put) a key that already exists
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use jammdb::{DB};
    /// # use jammdb::Error;
    ///
    /// # fn main() -> Result<(), Error> {
    /// let db = DB::open("my.db")?;
    /// let mut tx = db.tx(true)?;
    ///
    /// // create a root-level bucket
    /// let bucket = tx.create_bucket("my-bucket")?;
    /// // starts at 0
    /// assert_eq!(bucket.next_int(), 0);
    ///
    /// let next_int = bucket.next_int();
    /// bucket.put(next_int.to_be_bytes(), [0]);
    /// // auto-incremented after inserting a key / value pair
    /// assert_eq!(bucket.next_int(), 1);
    ///
    /// bucket.put(0_u64.to_be_bytes(), [0, 0]);
    /// // not incremented after updating a key / value pair
    /// assert_eq!(bucket.next_int(), 1);
    ///
    /// bucket.create_bucket("nested-bucket")?;
    /// // auto-incremented after creating a nested bucket
    /// assert_eq!(bucket.next_int(), 2);
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub fn next_int(&self) -> u64 {
        self.inner.borrow().meta.next_int
    }

    pub(crate) fn rebalance(&mut self) -> Result<BucketMeta> {
        self.inner.borrow_mut().rebalance()
    }

    pub(crate) fn write(&mut self, file: &mut File) -> Result<()> {
        self.inner.borrow_mut().write(file)
    }

    #[doc(hidden)]
    #[cfg_attr(tarpaulin, skip)]
    pub(crate) fn print(&self) {
        self.inner.borrow().print()
    }

    pub(crate) fn meta(&self) -> BucketMeta {
        self.inner.borrow().meta
    }

    /// Gets [`Data`] from a bucket.
    ///
    /// Returns `None` if the key does not exist. Otherwise returns `Some(Data)` representing either a
    /// key / value pair or a nested-bucket.
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
    ///
    /// let bucket = tx.get_bucket("my-bucket")?;
    ///
    /// match bucket.get("some-key") {
    ///     Some(data) => {
    ///         match &*data {
    ///             Data::Bucket(b) => println!("found a bucket with the name {:?}", b.name()),
    ///             Data::KeyValue(kv) => println!("found a kv pair {:?} {:?}", kv.key(), kv.value()),
    ///         }
    ///     },
    ///     None => println!("Key does not exist"),
    /// }
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub fn get<T: AsRef<[u8]>>(&self, key: T) -> Option<Ref<Data>> {
        let mut c = self.cursor();
        let exists = c.seek(key);
        if exists {
            c.current().map(Ref::new)
        } else {
            None
        }
    }

    /// Gets a key / value pair from a bucket.
    ///
    /// Returns `None` if the key does not exist, or if the key is for a nested bucket.
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
    ///
    /// let bucket = tx.get_bucket("my-bucket")?;
    /// bucket.create_bucket("sub-bucket")?;
    /// bucket.put("some-key", "some-value")?;
    ///
    /// if let Some(kv) = bucket.get_kv("some-key") {
    ///     assert_eq!(kv.value(), b"some-value");
    /// }
    /// assert!(bucket.get("sub-bucket").is_some());
    /// assert!(bucket.get_kv("sub-bucket").is_none());
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub fn get_kv<T: AsRef<[u8]>>(&self, key: T) -> Option<Ref<KVPair>> {
        match self.get(key) {
            Some(data) => match &*data {
                Data::KeyValue(kv) => Some(Ref::new(kv.clone())),
                _ => None,
            },
            _ => None,
        }
    }

    /// Iterator over the sub-buckets in this bucket.
    pub fn sub_buckets(&self) -> impl Iterator<Item = (Ref<BucketData>, BucketRef)> {
        Buckets { c: self.cursor() }
    }

    /// Iterator over the key / value pairs in this bucket.
    pub fn kv_pairs(&self) -> impl Iterator<Item = Ref<KVPair>> {
        KVPairs { c: self.cursor() }
    }
}

pub(crate) struct BucketInner {
    pub(crate) tx: Ptr<TransactionInner>,
    pub(crate) meta: BucketMeta,
    pub(crate) root: PageNodeID,
    dirty: bool,
    buckets: HashMap<Vec<u8>, Pin<Box<Bucket>>>,
    nodes: Vec<Pin<Box<Node>>>,
    page_node_ids: HashMap<PageID, NodeID>,
    page_parents: HashMap<PageID, PageID>,
}

impl BucketInner {
    fn new_child(&mut self, name: &[u8]) {
        self.dirty = true;
        let b = Bucket::new(BucketInner {
            tx: Ptr::new(&self.tx),
            meta: BucketMeta::default(),
            root: PageNodeID::Node(0),
            dirty: true,
            buckets: HashMap::new(),
            nodes: Vec::new(),
            page_node_ids: HashMap::new(),
            page_parents: HashMap::new(),
        });
        self.buckets.insert(Vec::from(name), Pin::new(Box::new(b)));
        let b = self.buckets.get_mut(name).unwrap();
        let mut b = b.inner.borrow_mut();
        let n = Node::new(0, Page::TYPE_LEAF, Ptr::new(&b));
        b.nodes.push(Pin::new(Box::new(n)));
        b.page_node_ids.insert(0, 0);
    }

    pub(crate) fn new_node(&mut self, data: NodeData) -> &mut Node {
        let node_id = self.nodes.len() as u64;
        let n = Node::with_data(node_id, data, Ptr::new(self));
        self.nodes.push(Pin::new(Box::new(n)));
        self.nodes.get_mut(node_id as usize).unwrap()
    }

    fn from_meta(&self, meta: BucketMeta) -> BucketInner {
        BucketInner {
            tx: Ptr::new(&self.tx),
            meta,
            root: PageNodeID::Page(meta.root_page),
            dirty: false,
            buckets: HashMap::new(),
            nodes: Vec::new(),
            page_node_ids: HashMap::new(),
            page_parents: HashMap::new(),
        }
    }

    fn create_bucket<T: AsRef<[u8]>>(&mut self, name: T) -> Result<BucketRef> {
        if !self.tx.writable {
            return Err(Error::ReadOnlyTx);
        }
        self.bucket_getter(name.as_ref(), true, true)
    }

    pub(crate) fn get_bucket<T: AsRef<[u8]>>(&mut self, name: T) -> Result<BucketRef> {
        self.bucket_getter(name.as_ref(), false, false)
    }

    fn get_or_create_bucket<T: AsRef<[u8]>>(&mut self, name: T) -> Result<BucketRef> {
        if !self.tx.writable {
            return Err(Error::ReadOnlyTx);
        }
        self.bucket_getter(name.as_ref(), true, false)
    }

    fn bucket_getter(
        &mut self,
        name: &[u8],
        should_create: bool,
        must_create: bool,
    ) -> Result<BucketRef> {
        let key = Vec::from(name);
        if !self.buckets.contains_key(&key) {
            let mut c = self.cursor();
            let exists = c.seek(name);
            let current_id = c.current_id();
            if !exists {
                if should_create {
                    self.meta.next_int += 1;
                    let key = Vec::from(name);
                    self.new_child(&key);
                    let data = {
                        let b = self.buckets.get(&key).unwrap();
                        let b = b.inner.borrow_mut();
                        let key = self.tx.copy_data(name);
                        Data::Bucket(BucketData::from_meta(key, &b.meta))
                    };
                    let node = self.node(current_id);
                    node.insert_data(data);
                } else {
                    return Err(Error::BucketMissing);
                }
            } else {
                match c.current() {
                    Some(data) => match data {
                        Data::Bucket(data) => {
                            if must_create {
                                return Err(Error::BucketExists);
                            }
                            let mut b = self.from_meta(data.meta());
                            b.meta = data.meta();
                            b.dirty = false;
                            self.buckets
                                .insert(key.clone(), Pin::new(Box::new(Bucket::new(b))));
                        }
                        _ => return Err(Error::IncompatibleValue),
                    },
                    None => return Err(Error::BucketMissing),
                }
            }
        } else if must_create {
            return Err(Error::BucketExists);
        }
        Ok(BucketRef::new(self.buckets.get(&key).unwrap()))
    }

    fn delete_bucket<T: AsRef<[u8]>>(&mut self, name: T) -> Result<()> {
        if !self.tx.writable {
            return Err(Error::ReadOnlyTx);
        }
        // make sure the bucket is in our map
        self.get_bucket(&name)?;
        // remove the bucket from the map so it will be dropped at the end of this function
        let b = self.buckets.remove(&Vec::from(name.as_ref())).unwrap();
        let b = Pin::into_inner(b);
        let b = b.inner.borrow_mut();
        // check that the bucket wasn't just created and never comitted
        let mut remaining_pages = Vec::new();
        if b.meta.root_page != 0 {
            // create a stack of pages to free and keep going until
            // we've freed every reachable page starting from this bucket's root page
            remaining_pages.push(b.meta.root_page);
            while !remaining_pages.is_empty() {
                let page_id = remaining_pages.pop().unwrap();
                let page = self.tx.page(page_id);
                let num_pages = page.overflow + 1;
                match page.page_type {
                    // every branch element's page much be freed
                    Page::TYPE_BRANCH => {
                        page.branch_elements()
                            .iter()
                            .for_each(|b| remaining_pages.push(b.page));
                    }
                    Page::TYPE_LEAF => {
                        // every nested bucket's pages must be freed
                        page.leaf_elements().iter().for_each(|leaf| {
                            if leaf.node_type == Node::TYPE_BUCKET {
                                let bucket_data = BucketData::new(leaf.key(), leaf.value());
                                remaining_pages.push(bucket_data.meta().root_page);
                            }
                        });
                    }
                    _ => (),
                }
                self.tx.free(page_id, num_pages);
            }
        }
        // delete the element from this bucket
        let mut c = self.cursor();
        let exists = c.seek(&name);
        if exists {
            let data = c.current().unwrap();
            let current_id = c.current_id();
            let index = c.current_index();
            if !data.is_kv() {
                self.dirty = true;
                let node = self.node(current_id);
                node.delete(index);
                Ok(())
            } else {
                Err(Error::IncompatibleValue)
            }
        } else {
            Err(Error::KeyValueMissing)
        }
    }

    fn put<T: AsRef<[u8]>, S: AsRef<[u8]>>(&mut self, key: T, value: S) -> Result<Option<KVPair>> {
        if !self.tx.writable {
            return Err(Error::ReadOnlyTx);
        }
        let k = self.tx.copy_data(key.as_ref());
        let v = self.tx.copy_data(value.as_ref());
        match self.put_data(Data::KeyValue(KVPair::from_slice_parts(k, v)))? {
            Some(data) => match data {
                Data::KeyValue(kv) => Ok(Some(kv)),
                _ => panic!("Unexpected data"),
            },
            None => Ok(None),
        }
    }

    fn delete<T: AsRef<[u8]>>(&mut self, key: T) -> Result<KVPair> {
        if !self.tx.writable {
            return Err(Error::ReadOnlyTx);
        }
        let mut c = self.cursor();
        let exists = c.seek(key);
        if exists {
            let data = c.current().unwrap();
            if data.is_kv() {
                let current_id = c.current_id();
                let index = c.current_index();
                self.dirty = true;
                let node = self.node(current_id);
                match node.delete(index) {
                    Data::KeyValue(kv) => Ok(kv),
                    _ => panic!("Unexpected data"),
                }
            } else {
                Err(Error::IncompatibleValue)
            }
        } else {
            Err(Error::KeyValueMissing)
        }
    }

    fn put_data(&mut self, data: Data) -> Result<Option<Data>> {
        let mut c = self.cursor();
        let exists = c.seek(data.key());
        let current_id = c.current_id();
        let current_data = if exists {
            let current = c.current().unwrap();
            if current.is_kv() != data.is_kv() {
                return Err(Error::IncompatibleValue);
            }
            Some(current)
        } else {
            self.meta.next_int += 1;
            None
        };
        let node = self.node(current_id);
        node.insert_data(data);
        self.dirty = true;
        Ok(current_data)
    }

    fn cursor(&self) -> Cursor {
        Cursor::new(Ptr::new(self))
    }

    pub(crate) fn page_node(&self, page: PageID) -> PageNode {
        if let Some(node_id) = self.page_node_ids.get(&page) {
            PageNode::Node(Ptr::new(self.nodes.get(*node_id as usize).unwrap()))
        } else {
            PageNode::Page(Ptr::new(self.tx.page(page)))
        }
    }

    pub(crate) fn add_page_parent(&mut self, page: PageID, parent: PageID) {
        debug_assert!(self.meta.root_page == parent || self.page_parents.contains_key(&parent));
        self.page_parents.insert(page, parent);
    }

    pub(crate) fn node(&mut self, id: PageNodeID) -> &mut Node {
        let id: NodeID = match id {
            PageNodeID::Page(page_id) => {
                if let Some(node_id) = self.page_node_ids.get(&page_id) {
                    return &mut self.nodes[*node_id as usize];
                }
                debug_assert!(
                    self.meta.root_page == page_id || self.page_parents.contains_key(&page_id)
                );
                let node_id = self.nodes.len() as u64;
                self.page_node_ids.insert(page_id, node_id);
                let n: Node = Node::from_page(node_id, Ptr::new(self), self.tx.page(page_id));
                self.nodes.push(Pin::new(Box::new(n)));
                if self.meta.root_page != page_id {
                    let node_key = self.nodes[node_id as usize].data.key_parts();
                    let parent = self.node(PageNodeID::Page(self.page_parents[&page_id]));
                    parent.insert_child(node_id, node_key);
                }
                node_id
            }
            PageNodeID::Node(id) => id,
        };
        self.nodes.get_mut(id as usize).unwrap()
    }

    fn is_dirty(&mut self) -> bool {
        if !self.dirty {
            for (_key, b) in self.buckets.iter_mut() {
                let b = b.inner.get_mut();
                if b.is_dirty() {
                    self.dirty = true;
                    break;
                }
            }
        }
        return self.dirty;
    }

    fn rebalance(&mut self) -> Result<BucketMeta> {
        let mut bucket_metas = HashMap::new();
        for (key, b) in self.buckets.iter_mut() {
            let b = b.inner.get_mut();
            if b.is_dirty() {
                self.dirty = true;
                let bucket_meta = b.rebalance()?;
                bucket_metas.insert(key.clone(), bucket_meta);
            }
        }
        for (k, b) in bucket_metas {
            let name = self.tx.copy_data(&k[..]);
            let meta = self.tx.copy_data(b.as_ref());
            self.put_data(Data::Bucket(BucketData::from_slice_parts(name, meta)))?;
        }
        if self.dirty {
            // merge emptyish nodes first
            {
                let mut root_node = self.node(self.root);
                let should_merge_root = root_node.merge();
                // check if the root is a bucket and only has one node
                if should_merge_root && !root_node.leaf() && root_node.data.len() == 1 {
                    // remove the branch and make the leaf node the root
                    root_node.free_page();
                    root_node.deleted = true;
                    let page_id = match &root_node.data {
                        NodeData::Branches(branches) => branches[0].page,
                        _ => panic!("uh wat"),
                    };
                    self.meta.root_page = page_id;
                    self.root = PageNodeID::Page(page_id);
                    // if the new root hasn't been modified, no need to split it
                    if !self.page_node_ids.contains_key(&page_id) {
                        self.dirty = false;
                        return Ok(self.meta);
                    }
                    // otherwise we'll continue to possibly split the new root
                    // this could result in re-adding a branch root node,
                    // but it's pretty unlikely!
                }
            }
            // split overflowing nodes
            {
                let mut root_node = self.node(self.root);
                while let Some(mut branches) = root_node.split() {
                    branches.insert(0, Branch::from_node(root_node));
                    root_node = self.new_node(NodeData::Branches(branches));
                }
                let page_id = root_node.page_id;
                self.root = PageNodeID::Node(root_node.page_id);
                self.meta.root_page = page_id;
            }
        }
        Ok(self.meta)
    }

    pub(crate) fn write(&mut self, file: &mut File) -> Result<()> {
        for (_, b) in self.buckets.iter_mut() {
            b.inner.get_mut().write(file)?;
        }
        if self.dirty {
            for node in self.nodes.iter_mut() {
                if !node.deleted {
                    node.write(file)?;
                }
            }
        }
        Ok(())
    }

    #[doc(hidden)]
    #[cfg_attr(tarpaulin, skip)]
    fn print(&self) {
        let page = self.tx.page(self.meta.root_page);
        page.print(&self.tx);
    }
}

pub struct BucketRef<'a> {
    bucket: *const Bucket,
    _phantom: PhantomData<&'a ()>,
}

impl<'a> BucketRef<'a> {
    pub(crate) fn new(b: &Bucket) -> BucketRef {
        BucketRef {
            bucket: b as *const Bucket,
            _phantom: PhantomData {},
        }
    }

    pub(crate) fn from_ptr<'b>(b: *const Bucket) -> BucketRef<'b> {
        BucketRef {
            bucket: b,
            _phantom: PhantomData {},
        }
    }
}

impl<'a> Deref for BucketRef<'a> {
    type Target = Bucket;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.bucket }
    }
}

impl<'a> DerefMut for BucketRef<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *(self.bucket as *mut Bucket) }
    }
}

const META_SIZE: usize = std::mem::size_of::<BucketMeta>();

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct BucketMeta {
    pub(crate) root_page: PageID,
    pub(crate) next_int: u64,
}

impl AsRef<[u8]> for BucketMeta {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        let ptr = self as *const BucketMeta as *const u8;
        unsafe { std::slice::from_raw_parts(ptr, META_SIZE) }
    }
}

// pub struct BucketIter<'a> {
//     b: &'a Bucket,
//     i: Buckets<'a>,
// }

// impl<'a> Iterator for BucketIter<'a> {
//     type Item = BucketRef<'a>;

//     fn next(&mut self) -> Option<Self::Item> {
//         match self.i.next() {
//             Some(b) => self.b.get_bucket(b.name()).map(|b| Some(b)).unwrap_or(None),
//             None => None,
//         }
//     }
// }

// #[cfg(test)]
// mod tests {
// 	use super::*;
// 	use crate::db::DB;
// 	use crate::testutil::RandomFile;

// 	#[test]
// 	fn test_incompatible_values() -> Result<()> {
// 		let random_file = RandomFile::new();
// 		let db = DB::open(&random_file)?;
// 		{
// 			let tx = db.tx(true)?;
// 			assert_eq!(tx.get_bucket("abc").err(), Some(Error::BucketMissing));
// 			let b = tx.create_bucket("abc")?;
// 			b.put("key", "value")?;
// 			assert_eq!(b.create_bucket("key").err(), Some(Error::IncompatibleValue));
// 			b.create_bucket("nested-bucket")?;
// 			assert_eq!(
// 				b.put("nested-bucket", "value"),
// 				Err(Error::IncompatibleValue)
// 			);
// 			assert_eq!(
// 				b.create_bucket("nested-bucket").err(),
// 				Some(Error::BucketExists)
// 			);

// 			assert_eq!(b.delete("missing-key"), Err(Error::KeyValueMissing));
// 			tx.commit()?;
// 		}
// 		{
// 			let tx = db.tx(true)?;
// 			let b = tx.get_bucket("abc")?;
// 			assert_eq!(b.create_bucket("key").err(), Some(Error::IncompatibleValue));
// 			assert_eq!(
// 				b.put("nested-bucket", "value"),
// 				Err(Error::IncompatibleValue)
// 			);
// 		}
// 		db.check()
// 	}

// 	#[test]
// 	fn test_get_kv() -> Result<()> {
// 		let random_file = RandomFile::new();
// 		let db = DB::open(&random_file)?;
// 		{
// 			let mut tx = db.tx(true)?;
// 			let mut b = tx.create_bucket("abc")?;
// 			b.create_bucket("nested-bucket")?;
// 			b.put("key", "value")?;
// 			assert_eq!(b.get_kv("key").unwrap().value(), b"value");
// 			assert!(b.get_kv("nested-bucket").is_none());
// 			assert!(b.get("nested-bucket").is_some());
// 			tx.commit()?;
// 		}
// 		{
// 			let mut tx = db.tx(false)?;
// 			let b = tx.get_bucket("abc")?;
// 			assert_eq!(b.get_kv("key").unwrap().value(), b"value");
// 			assert!(b.get_kv("nested-bucket").is_none());
// 			assert!(b.get("nested-bucket").is_some());
// 		}
// 		db.check()
// 	}
// }
