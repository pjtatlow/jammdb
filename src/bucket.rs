use std::{
    cell::{RefCell, RefMut},
    collections::HashMap,
    rc::Rc,
};

use crate::{
    bytes::{Bytes, ToBytes},
    cursor::{search, Buckets, Cursor, KVPairs},
    data::{BucketData, Data, KVPair},
    errors::{Error, Result},
    freelist::TxFreelist,
    node::{Node, NodeData, NodeID},
    page::{Page, PageID, Pages},
    page_node::{PageNode, PageNodeID},
};

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
///     match data {
///         Data::Bucket(b) => println!("found a bucket with the name {:?}", b.name()),
///         Data::KeyValue(kv) => println!("found a kv pair {:?} {:?}", kv.key(), kv.value()),
///     }
/// }
///
/// println!("Bucket next_int {:?}", bucket.next_int());
/// # Ok(())
/// # }
/// ```
pub struct Bucket<'b> {
    pub(crate) inner: Rc<RefCell<InnerBucket<'b>>>,
    pub(crate) freelist: Rc<RefCell<TxFreelist>>,
    pub(crate) writable: bool,
}

impl<'b> Bucket<'b> {
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
    pub fn put<T: ToBytes<'b>, S: ToBytes<'b>>(&self, key: T, value: S) -> Result<Option<KVPair>> {
        if !self.writable {
            return Err(Error::ReadOnlyTx);
        }
        let mut b = self.inner.borrow_mut();
        b.put(key, value)
    }

    pub fn get<T: AsRef<[u8]>>(&self, key: T) -> Option<Data> {
        let mut b = self.inner.borrow_mut();
        b.get(key)
    }

    pub fn get_kv<T: AsRef<[u8]>>(&self, key: T) -> Option<KVPair> {
        let mut b = self.inner.borrow_mut();
        b.get_kv(key)
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
    pub fn delete<T: AsRef<[u8]>>(&self, key: T) -> Result<KVPair> {
        if !self.writable {
            return Err(Error::ReadOnlyTx);
        }
        let mut b = self.inner.borrow_mut();
        b.delete(key)
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
    pub fn get_bucket<'a, T: ToBytes<'b>>(&'a self, name: T) -> Result<Bucket<'b>> {
        let mut b = self.inner.borrow_mut();
        let inner = b.get_bucket(name)?;
        Ok(Bucket {
            inner,
            freelist: self.freelist.clone(),
            writable: self.writable,
        })
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
    pub fn create_bucket<'a, T: ToBytes<'b>>(&'a self, name: T) -> Result<Bucket<'b>> {
        if !self.writable {
            return Err(Error::ReadOnlyTx);
        }
        let mut b = self.inner.borrow_mut();
        let inner = b.create_bucket(name)?;
        Ok(Bucket {
            inner,
            freelist: self.freelist.clone(),
            writable: self.writable,
        })
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
    pub fn get_or_create_bucket<'a, T: ToBytes<'b>>(&'a self, name: T) -> Result<Bucket<'b>> {
        if !self.writable {
            return Err(Error::ReadOnlyTx);
        }
        let mut b = self.inner.borrow_mut();
        let inner = b.get_or_create_bucket(name)?;
        Ok(Bucket {
            inner,
            freelist: self.freelist.clone(),
            writable: self.writable,
        })
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
    pub fn delete_bucket<T: ToBytes<'b>>(&self, key: T) -> Result<()> {
        if !self.writable {
            return Err(Error::ReadOnlyTx);
        }

        let mut freelist = self.freelist.borrow_mut();
        let mut b = self.inner.borrow_mut();
        b.delete_bucket(key, &mut freelist)
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
    ///     match data {
    ///         Data::Bucket(b) => println!("found a bucket with the name {:?}", b.name()),
    ///         Data::KeyValue(kv) => println!("found a kv pair {:?} {:?}", kv.key(), kv.value()),
    ///     }
    /// }
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub fn cursor<'a>(&'a self) -> Cursor<'b> {
        Cursor::new(self)
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

    /// Iterator over the sub-buckets in this bucket.
    pub fn buckets<'a>(&'a self) -> Buckets<'b> {
        Buckets { c: self.cursor() }
    }

    /// Iterator over the key / value pairs in this bucket.
    pub fn kv_pairs<'a>(&'a self) -> KVPairs<'b> {
        KVPairs { c: self.cursor() }
    }
}

pub(crate) struct InnerBucket<'b> {
    root: PageNodeID,
    dirty: bool,
    buckets: HashMap<Bytes<'b>, Rc<RefCell<InnerBucket<'b>>>>,
    pub(crate) nodes: Vec<Rc<RefCell<Node<'b>>>>,
    // Maps a PageID to it's NodeID, so we don't create multiple nodes for a single page
    page_node_ids: HashMap<PageID, NodeID>,
    // Maps PageIDs to their parent's PageID
    page_parents: HashMap<PageID, PageID>,
    pages: Pages,
    pub(crate) meta: BucketMeta,
}

impl<'b> InnerBucket<'b> {
    pub(crate) fn from_meta(meta: BucketMeta, pages: Pages) -> InnerBucket<'b> {
        debug_assert!(
            meta.root_page > 1,
            "bucket cannot have root page {}, reserved for meta",
            meta.root_page
        );
        InnerBucket {
            meta,
            root: PageNodeID::Page(meta.root_page),
            dirty: false,
            buckets: HashMap::new(),
            nodes: Vec::new(),
            page_node_ids: HashMap::new(),
            page_parents: HashMap::new(),
            pages,
        }
    }

    fn new_child<'a>(&'a mut self, name: Bytes<'b>) -> RefMut<InnerBucket<'b>> {
        self.dirty = true;
        let n = Node::new(0, Page::TYPE_LEAF, self.pages.pagesize);
        let mut page_node_ids = HashMap::new();
        page_node_ids.insert(0, 0);
        let b = InnerBucket {
            meta: BucketMeta::default(),
            root: PageNodeID::Node(0),
            dirty: true,
            buckets: HashMap::new(),
            nodes: vec![Rc::new(RefCell::new(n))],
            page_node_ids,
            page_parents: HashMap::new(),
            pages: self.pages.clone(),
        };
        self.buckets.insert(name.clone(), Rc::new(RefCell::new(b)));
        let b = self.buckets.get_mut(&name).unwrap();
        b.borrow_mut()
    }

    pub(crate) fn add_page_parent(&mut self, page: PageID, parent: PageID) {
        debug_assert!(
            self.meta.root_page == parent || self.page_parents.contains_key(&parent),
            "cannot find reference to parent page ID \"{}\"",
            parent
        );
        self.page_parents.insert(page, parent);
    }

    pub(crate) fn page_node<'a>(&'a self, id: PageNodeID) -> PageNode<'b> {
        match id {
            PageNodeID::Page(page) => {
                if let Some(node_id) = self.page_node_ids.get(&page) {
                    PageNode::Node(self.nodes[*node_id as usize].clone())
                } else {
                    PageNode::Page(self.pages.page(page))
                }
            }
            PageNodeID::Node(node) => PageNode::Node(self.nodes[node as usize].clone()),
        }
    }

    pub fn get<'a, T: AsRef<[u8]>>(&'a mut self, key: T) -> Option<Data<'b>> {
        let (exists, stack) = search(key.as_ref(), self.meta.root_page, self);
        let last = stack.last().unwrap();
        if exists {
            let page_node = self.page_node(last.id);
            page_node.val(last.index)
        } else {
            None
        }
    }

    pub fn get_kv<'a, T: AsRef<[u8]>>(&'a mut self, key: T) -> Option<KVPair<'b>> {
        match self.get(key) {
            Some(Data::KeyValue(kv)) => Some(kv),
            _ => None,
        }
    }

    pub fn put<'a, T: ToBytes<'b>, S: ToBytes<'b>>(
        &'a mut self,
        key: T,
        value: S,
    ) -> Result<Option<KVPair<'b>>> {
        let k = key.to_bytes();
        let v = value.to_bytes();

        match self.put_data(Data::KeyValue(KVPair::new(k, v)))? {
            Some(data) => match data {
                Data::KeyValue(kv) => Ok(Some(kv)),
                _ => panic!("Unexpected data"),
            },
            None => Ok(None),
        }
    }

    fn delete<'a, T: AsRef<[u8]>>(&'a mut self, key: T) -> Result<KVPair<'b>> {
        let (exists, stack) = search(key.as_ref(), self.meta.root_page, self);
        let last = stack.last().unwrap();
        if exists {
            let page_node = self.page_node(last.id);
            let data = page_node.val(last.index).unwrap();
            if data.is_kv() {
                let current_id = last.id;
                let index = last.index;
                self.dirty = true;
                let node = self.node(current_id, None);
                let mut node = node.borrow_mut();
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

    fn put_data<'a>(&'a mut self, data: Data<'b>) -> Result<Option<Data<'b>>> {
        let (exists, stack) = search(data.key(), self.meta.root_page, self);
        let last = stack.last().unwrap();
        let current_data = if exists {
            let page_node = self.page_node(last.id);
            let current = page_node.val(last.index).unwrap();
            if current.is_kv() != data.is_kv() {
                return Err(Error::IncompatibleValue);
            }
            Some(current)
        } else {
            self.meta.next_int += 1;
            None
        };
        let node = self.node(last.id, None);
        let mut node = node.borrow_mut();
        node.insert_data(data);
        self.dirty = true;

        Ok(current_data)
    }

    pub(crate) fn create_bucket<T: ToBytes<'b>>(&mut self, name: T) -> Result<Rc<RefCell<Self>>> {
        self.bucket_getter(name.to_bytes(), true, true)
    }

    pub(crate) fn get_bucket<'a, T: ToBytes<'b>>(
        &'a mut self,
        name: T,
    ) -> Result<Rc<RefCell<Self>>> {
        self.bucket_getter(name.to_bytes(), false, false)
    }

    pub(crate) fn get_or_create_bucket<T: ToBytes<'b>>(
        &mut self,
        name: T,
    ) -> Result<Rc<RefCell<Self>>> {
        self.bucket_getter(name.to_bytes(), true, false)
    }

    fn bucket_getter<'a>(
        &'a mut self,
        name: Bytes<'b>,
        should_create: bool,
        must_create: bool,
    ) -> Result<Rc<RefCell<InnerBucket<'b>>>> {
        if !self.buckets.contains_key(&name) {
            let (exists, stack) = search(name.as_ref(), self.meta.root_page, self);
            let last = stack.last().unwrap();
            if !exists {
                if should_create {
                    self.meta.next_int += 1;
                    let data = {
                        let b = self.new_child(name.clone());
                        let meta = b.meta;
                        Data::Bucket(BucketData::new(name.clone(), meta))
                    };
                    let node = self.node(last.id, None);
                    let mut node = node.borrow_mut();
                    node.insert_data(data);
                } else {
                    return Err(Error::BucketMissing);
                }
            } else {
                let page_node = self.page_node(last.id);
                match page_node.val(last.index) {
                    Some(data) => match data {
                        Data::Bucket(data) => {
                            if must_create {
                                return Err(Error::BucketExists);
                            }
                            let b = Self::from_meta(data.meta(), self.pages.clone());
                            self.buckets.insert(name.clone(), Rc::new(RefCell::new(b)));
                        }
                        _ => return Err(Error::IncompatibleValue),
                    },
                    None => return Err(Error::BucketMissing),
                }
            }
        } else if must_create {
            return Err(Error::BucketExists);
        }
        Ok(self.buckets.get(&name).unwrap().clone())
    }

    pub(crate) fn delete_bucket<T: ToBytes<'b>>(
        &mut self,
        name: T,
        freelist: &mut TxFreelist,
    ) -> Result<()> {
        let name = name.to_bytes();
        // make sure the bucket is in our map
        self.get_bucket(name.clone())?;

        // remove the bucket from the map so it will be dropped at the end of this function
        let bucket = self.buckets.remove(&name).unwrap();
        let b = bucket.borrow_mut();
        // check that the bucket wasn't just created and never comitted
        let mut remaining_pages = Vec::new();
        if b.meta.root_page != 0 {
            // create a stack of pages to free and keep going until
            // we've freed every reachable page starting from this bucket's root page
            remaining_pages.push(b.meta.root_page);
            while !remaining_pages.is_empty() {
                let page_id = remaining_pages.pop().unwrap();
                let page = self.pages.page(page_id);
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
                                let bucket_data: BucketData = leaf.into();
                                remaining_pages.push(bucket_data.meta().root_page);
                            }
                        });
                    }
                    _ => (),
                }
                freelist.free(page_id, num_pages);
            }
        }
        // delete the element from this bucket
        let (exists, stack) = search(name.as_ref(), self.meta.root_page, self);
        let last = stack.last().unwrap();
        if exists {
            let page_node = self.page_node(last.id);
            let data = page_node.val(last.index).unwrap();

            if !data.is_kv() {
                self.dirty = true;
                let current_id = last.id;
                let index = last.index;
                let node = self.node(current_id, None);
                let mut node = node.borrow_mut();
                node.delete(index);
                Ok(())
            } else {
                Err(Error::IncompatibleValue)
            }
        } else {
            panic!("Did not find data for bucket we already deleted")
        }
    }

    pub(crate) fn node<'a>(
        &'a mut self,
        id: PageNodeID,
        parent: Option<&mut Node>,
    ) -> Rc<RefCell<Node<'b>>> {
        let id: NodeID = match id {
            PageNodeID::Page(page_id) => {
                if let Some(node_id) = self.page_node_ids.get(&page_id) {
                    return self.nodes[*node_id as usize].clone();
                }
                debug_assert!(
                    self.meta.root_page == page_id || self.page_parents.contains_key(&page_id),
                    "cannot find reference to page ID \"{}\"",
                    page_id,
                );
                let node_id = self.nodes.len() as u64;
                self.page_node_ids.insert(page_id, node_id);
                let n: Node =
                    Node::from_page(node_id, self.pages.page(page_id), self.pages.pagesize);
                self.nodes.push(Rc::new(RefCell::new(n)));
                // If this node is not for the root page, then recursively create nodes for the parent pages
                if self.meta.root_page != page_id {
                    let n = self.nodes[node_id as usize].clone();
                    let mut n = n.borrow_mut();
                    let node_key = n.data.first_key();
                    if let Some(parent) = parent {
                        parent.insert_child(node_id, node_key);
                        n.parent = Some(parent.id);
                    } else {
                        let parent = self.node(PageNodeID::Page(self.page_parents[&page_id]), None);
                        let mut parent = parent.borrow_mut();
                        parent.insert_child(node_id, node_key);
                        n.parent = Some(parent.id);
                    }
                }
                node_id
            }
            PageNodeID::Node(id) => id,
        };
        self.nodes.get_mut(id as usize).unwrap().clone()
    }

    pub(crate) fn new_node<'a>(&'a mut self, data: NodeData<'b>) -> Rc<RefCell<Node<'b>>> {
        debug_assert!(data.len() >= 2);
        let node_id = self.nodes.len() as u64;
        let n = Node::with_data(node_id, data, self.pages.pagesize);
        self.nodes.push(Rc::new(RefCell::new(n)));
        self.nodes[node_id as usize].clone()
    }

    fn is_dirty(&mut self) -> bool {
        // If it isn't marked as dirty, make sure by checking
        // the sub-buckets to see if they're dirty.
        if !self.dirty {
            for (_key, b) in self.buckets.iter() {
                let mut b = b.borrow_mut();
                if b.is_dirty() {
                    self.dirty = true;
                    break;
                }
            }
        }
        self.dirty
    }

    // Make sure none of the nodes are too empty
    pub(crate) fn rebalance(&mut self, tx_freelist: &mut TxFreelist) -> Result<()> {
        if !self.is_dirty() {
            return Ok(());
        }
        for b in self.buckets.values() {
            let mut b = b.borrow_mut();
            b.rebalance(tx_freelist)?;
        }

        // merge emptyish nodes with siblings
        self.merge_nodes(tx_freelist);

        Ok(())
    }

    fn merge_nodes(&mut self, tx_freelist: &mut TxFreelist) {
        // If we haven't initialized any nodes yet, make sure we have the root node.
        // If there is even one node, we are guarunteed to hage loaded the root node too.
        if self.page_node_ids.len() == 0 {
            self.node(PageNodeID::Page(self.meta.root_page), None);
        }
        let mut stack: Vec<(bool, u64)> = vec![(false, self.page_node_ids[&self.meta.root_page])];

        while let Some((visited, node_id)) = stack.pop() {
            let node = self.nodes[node_id as usize].clone();
            let mut node = node.borrow_mut();
            // If this is a leaf node or our second time visiting a branch node, try to merge it
            if visited || node.leaf() {
                // Do nothing if this node needs no merging
                if !node.needs_merging() {
                    continue;
                }
                // Handle root node speially
                if node.page_id == self.meta.root_page {
                    // If the root node has only one branch, promote that page to the root page
                    if !node.leaf() && node.data.len() == 1 {
                        // delete the root node
                        node.free_page(tx_freelist);
                        node.deleted = true;
                        let page_id = if let NodeData::Branches(branches) = &node.data {
                            branches[0].page
                        } else {
                            // We already know it was a branch node, so we can't get here.
                            unreachable!()
                        };
                        // Just double check that the child page wasn't accidentally pointing at a meta page
                        debug_assert!(
                            page_id > 1,
                            "cannot have page <= 1, those are reserved for metadata"
                        );
                        // Make that child page the bucket's root page.
                        self.meta.root_page = page_id;
                        self.root = PageNodeID::Page(page_id);
                    }
                } else {
                    // else find a sibling and merge this node with that one
                    let parent_id = node.parent.expect("non root node must have parent");
                    let parent_ref = self.nodes[parent_id as usize].clone();

                    // borrow the parent in a separate scope so we can drop it before we initialize the sibling node
                    let mut parent = parent_ref.borrow_mut();
                    if let NodeData::Branches(branches) = &mut parent.data {
                        // If there is only one branch in the parent, then we cannot delete this node
                        // since there are no siblings to move the data to.
                        // When we handle the parent, it will get merged with it's siblings or promoted
                        // to root.
                        if branches.len() == 1 {
                            continue;
                        }
                        // check if there is any data left to copy
                        // find the child's branch element in the parent node's data
                        let index = match branches.binary_search_by_key(
                            &node.original_key.clone().unwrap().as_ref(),
                            |b| b.key(),
                        ) {
                            Ok(i) => i,
                            _ => panic!("child branch not found"),
                        };
                        if node.data.len() > 0 && branches.len() > 1 {
                            // add that child's data to a sibling node
                            let sibling_page = if index == 0 {
                                // right sibling
                                branches[index + 1].page
                            } else {
                                // left sibling
                                branches[index - 1].page
                            };

                            self.page_parents.insert(sibling_page, parent.page_id);
                            let sibling =
                                self.node(PageNodeID::Page(sibling_page), Some(&mut parent));

                            let mut sibling = sibling.borrow_mut();
                            // Copy this node's data over to it's sibling
                            sibling.data.merge(&mut node.data);
                            // Move all children nodes over to that sibling too
                            for child in node.children.iter() {
                                let c = &mut self.nodes[*child as usize];
                                let mut c = c.borrow_mut();
                                c.parent = Some(sibling.id);
                            }
                            sibling.children.append(&mut node.children);
                        }
                        // free the child's page and mark it as deleted
                        node.free_page(tx_freelist);
                        node.deleted = true;
                        if let NodeData::Branches(branches) = &mut parent.data {
                            // remove the child from this node
                            branches.remove(index);
                        }
                        if let Some(i) = parent.children.iter().position(|x| *x == node.id) {
                            parent.children.remove(i);
                        };
                    }
                }
            } else {
                // Add self back to stack to be processed after children
                stack.push((true, node_id));
                // Add all children to the stack, in reverse order so we pop them off
                // the stack from left to right
                for id in node.children.iter().rev() {
                    stack.push((false, *id));
                }
            }
        }
    }

    // Make sure none of the nodes are too full, creating other nodes as needed.
    // Then, write all of those nodes to dirty pages.
    pub(crate) fn spill(&mut self, tx_freelist: &mut TxFreelist) -> Result<BucketMeta> {
        if !self.is_dirty() {
            return Ok(self.meta);
        }

        #[allow(clippy::mutable_key_type)]
        let mut bucket_metas: HashMap<Bytes, BucketMeta> = HashMap::new();
        for (key, b) in self.buckets.iter() {
            let mut b = b.borrow_mut();
            let bucket_meta = b.spill(tx_freelist)?;
            // Store updated bucket metadata in a map since self is borrowed
            bucket_metas.insert(key.clone(), bucket_meta);
        }
        // Update our pointers to the sub-buckets' new pages
        for (name, meta) in bucket_metas {
            self.put_data(Data::Bucket(BucketData::new(name, meta)))?;
        }

        let root = self.nodes[self.page_node_ids[&self.meta.root_page] as usize].clone();
        let mut root = root.borrow_mut();
        let page_id = root
            .spill(self, tx_freelist, None)?
            .expect("root node did not return a new page_id");
        self.meta.root_page = page_id;

        Ok(self.meta)
    }
}

pub const META_SIZE: usize = std::mem::size_of::<BucketMeta>();

#[repr(C)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
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
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytes() {
        let meta = BucketMeta {
            root_page: 3,
            next_int: 1,
        };
        let bytes = meta.as_ref();
        assert_eq!(bytes, &[3, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0]);
    }
}
