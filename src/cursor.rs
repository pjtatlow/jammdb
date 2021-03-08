use std::marker::PhantomData;

use crate::node::{Node, NodeData, NodeID};
use crate::page::{Page, PageID};
use crate::ptr::Ptr;
use crate::{
    bucket::{BucketInner, BucketRef},
    Bucket,
};
use crate::{
    data::{Data, KVPair, Ref},
    BucketData,
};

#[derive(Clone, Copy)]
pub(crate) enum PageNodeID {
    Page(PageID),
    Node(NodeID),
}

pub(crate) enum PageNode {
    Page(Ptr<Page>),
    Node(Ptr<Node>),
}

impl PageNode {
    fn leaf(&self) -> bool {
        match self {
            PageNode::Page(p) => p.page_type == Page::TYPE_LEAF,
            PageNode::Node(n) => n.leaf(),
        }
    }

    fn len(&self) -> usize {
        match self {
            PageNode::Page(p) => p.count as usize,
            PageNode::Node(n) => n.data.len(),
        }
    }

    fn index_page(&self, index: usize) -> PageID {
        match self {
            PageNode::Page(p) => {
                if index >= p.count as usize {
                    return 0;
                }
                match p.page_type {
                    Page::TYPE_BRANCH => p.branch_elements()[index].page,
                    _ => panic!("INVALID PAGE TYPE FOR INDEX_PAGE"),
                }
            }
            PageNode::Node(n) => {
                if index >= n.data.len() {
                    return 0;
                }
                match &n.data {
                    NodeData::Branches(b) => b[index].page,
                    _ => panic!("INVALID NODE TYPE FOR INDEX_PAGE"),
                }
            }
        }
    }

    fn index(&self, key: &[u8]) -> (usize, bool) {
        let result = match self {
            PageNode::Page(p) => match p.page_type {
                Page::TYPE_LEAF => p.leaf_elements().binary_search_by_key(&key, |e| e.key()),
                Page::TYPE_BRANCH => p.branch_elements().binary_search_by_key(&key, |e| e.key()),
                _ => panic!("INVALID PAGE TYPE FOR INDEX: {:?}", p.page_type),
            },
            PageNode::Node(n) => match &n.data {
                NodeData::Branches(b) => b.binary_search_by_key(&key, |b| b.key()),
                NodeData::Leaves(l) => l.binary_search_by_key(&key, |l| l.key()),
            },
        };
        match result {
            Ok(i) => (i, true),
            // we didn't find the element, so point at the element just "before" the missing element
            Err(mut i) => {
                if i > 0 {
                    i -= 1;
                };
                (i, false)
            }
        }
    }

    fn val(&self, index: usize) -> Option<Data> {
        match self {
            PageNode::Page(p) => match p.page_type {
                Page::TYPE_LEAF => p.leaf_elements().get(index).map(|e| Data::from_leaf(e)),
                _ => panic!("INVALID PAGE TYPE FOR VAL"),
            },
            PageNode::Node(n) => match &n.data {
                NodeData::Leaves(l) => l.get(index).cloned(),
                _ => panic!("INVALID NODE TYPE FOR VAL"),
            },
        }
    }
}

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
///     match &*data {
///         Data::Bucket(b) => println!("found a bucket with the name {:?}", b.name()),
///         Data::KeyValue(kv) => println!("found a kv pair {:?} {:?}", kv.key(), kv.value()),
///     }
/// }
///
/// let mut cursor = bucket.cursor();
/// // seek to the key "f"
/// // if it doesn't exist, it will start at the position wh
/// cursor.seek("f");
/// //
/// for data in cursor {
/// }
///
/// # Ok(())
/// # }
/// ```
pub struct Cursor<'a> {
    bucket: Ptr<BucketInner>,
    stack: Vec<Elem>,
    next_called: bool,
    _phantom: PhantomData<&'a ()>,
}

impl<'a> Cursor<'a> {
    pub(crate) fn new(b: Ptr<BucketInner>) -> Cursor<'a> {
        Cursor {
            bucket: b,
            stack: vec![],
            next_called: false,
            _phantom: PhantomData {},
        }
    }

    pub(crate) fn current_id(&self) -> PageNodeID {
        let e = self.stack.last().unwrap();
        match &e.page_node {
            PageNode::Page(p) => PageNodeID::Page(p.id),
            PageNode::Node(n) => PageNodeID::Node(n.id),
        }
    }

    pub(crate) fn current_index(&self) -> usize {
        let e = self.stack.last().unwrap();
        e.index
    }

    /// Moves the cursor to the given key.
    /// If the key does not exist, the cursor stops "just before"
    /// where the key _would_ be.
    ///
    /// Returns whether or not the key exists in the bucket.
    pub fn seek<T: AsRef<[u8]>>(&mut self, key: T) -> bool {
        self.next_called = false;
        self.stack.clear();
        self.search(key.as_ref(), self.bucket.meta.root_page)
    }

    /// Returns the data at the cursor's current position.
    /// You can use this to get data after doing a [`seek`](#method.seek).
    pub fn current(&self) -> Option<Data> {
        match self.stack.last() {
            Some(e) => e.page_node.val(e.index),
            None => None,
        }
    }

    // recursive function that searches the bucket for a given key
    fn search(&mut self, key: &[u8], page_id: PageID) -> bool {
        let page_node = self.bucket.page_node(page_id);
        let (index, exact) = page_node.index(key);
        let leaf = page_node.leaf();
        self.stack.push(Elem { index, page_node });
        if leaf {
            return exact;
        }

        let next_page_id = self.stack.last().unwrap().page_node.index_page(index);
        if next_page_id == 0 {
            return false;
        }
        self.bucket.add_page_parent(next_page_id, page_id);

        self.search(key, next_page_id)
    }

    pub(crate) fn seek_first(&mut self) {
        if self.stack.is_empty() {
            let page_node = self.bucket.page_node(self.bucket.meta.root_page);
            self.stack.push(Elem {
                index: 0,
                page_node,
            });
        }
        loop {
            let elem = self.stack.last().unwrap();
            if elem.page_node.leaf() {
                break;
            }
            if elem.page_node.len() == 0 {
                break;
            }
            let page_node = self.bucket.page_node(elem.page_node.index_page(elem.index));
            self.stack.push(Elem {
                index: 0,
                page_node,
            });
        }
    }
}

impl<'a> Iterator for Cursor<'a> {
    type Item = Ref<'a, Data>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.stack.is_empty() {
            self.seek_first();
        } else if self.next_called {
            loop {
                let elem = self.stack.last_mut().unwrap();
                if elem.index >= (elem.page_node.len() - 1) {
                    if self.stack.len() == 1 {
                        return None;
                    }
                    self.stack.pop();
                    continue;
                } else {
                    elem.index += 1;
                }
                self.seek_first();
                break;
            }
        }
        self.next_called = true;
        self.current().map(Ref::new)
    }
}

struct Elem {
    index: usize,
    page_node: PageNode,
}

pub struct Buckets<'a> {
    pub(crate) c: Cursor<'a>,
}

impl<'a> Iterator for Buckets<'a> {
    type Item = (Ref<'a, BucketData>, BucketRef<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        match self.c.next() {
            Some(r) => match &*r {
                Data::Bucket(b) => match self.c.bucket.get_bucket(b.name()) {
                    Ok(r) => Some((
                        Ref::new(b.clone()),
                        BucketRef::from_ptr::<'a>((&*r) as *const Bucket),
                    )),
                    _ => None,
                },
                _ => None,
            },
            _ => None,
        }
    }
}

pub struct KVPairs<'a> {
    pub(crate) c: Cursor<'a>,
}

impl<'a> Iterator for KVPairs<'a> {
    type Item = Ref<'a, KVPair>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.c.next() {
            Some(r) => match &*r {
                Data::KeyValue(kv) => Some(Ref::new(kv.clone())),
                _ => None,
            },
            _ => None,
        }
    }
}
