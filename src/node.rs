use std::fs::File;
use std::io::{Seek, SeekFrom, Write};
use std::mem::size_of;

use crate::bucket::BucketInner;
use crate::cursor::PageNodeID;
use crate::data::{Data, SliceParts};
use crate::errors::Result;
use crate::page::{BranchElement, LeafElement, Page, PageID, PageType};
use crate::ptr::Ptr;

pub(crate) type NodeID = u64;

const HEADER_SIZE: u64 = size_of::<Page>() as u64;
const LEAF_SIZE: u64 = size_of::<LeafElement>() as u64;
const BRANCH_SIZE: u64 = size_of::<BranchElement>() as u64;
const MIN_KEYS_PER_NODE: usize = 2;
const FILL_PERCENT: f32 = 0.5;

pub(crate) struct Node {
    pub(crate) id: NodeID,
    pub(crate) page_id: PageID,
    pub(crate) num_pages: u64,
    bucket: Ptr<BucketInner>,
    // pub (crate) key: SliceParts,
    pub(crate) children: Vec<NodeID>,
    pub(crate) data: NodeData,
    pub(crate) deleted: bool,
    original_key: Option<SliceParts>,
}

pub(crate) enum NodeData {
    Branches(Vec<Branch>),
    Leaves(Vec<Data>),
}

impl NodeData {
    pub(crate) fn len(&self) -> usize {
        match self {
            NodeData::Branches(b) => b.len(),
            NodeData::Leaves(l) => l.len(),
        }
    }

    fn size(&self) -> u64 {
        match self {
            NodeData::Branches(b) => b
                .iter()
                .fold(BRANCH_SIZE * b.len() as u64, |acc, b| acc + b.key_size()),
            NodeData::Leaves(l) => l
                .iter()
                .fold(LEAF_SIZE * l.len() as u64, |acc, l| acc + l.size()),
        }
    }

    pub(crate) fn key_parts(&self) -> SliceParts {
        debug_assert!(self.len() > 0, "Cannot get key parts of empty data");
        match self {
            NodeData::Branches(b) => b.first().map(|b| b.key),
            NodeData::Leaves(l) => l.first().map(|l| l.key_parts()),
        }
        .unwrap()
    }

    fn split_at(&mut self, index: usize) -> NodeData {
        match self {
            NodeData::Branches(b) => NodeData::Branches(b.split_off(index)),
            NodeData::Leaves(l) => NodeData::Leaves(l.split_off(index)),
        }
    }

    fn merge(&mut self, other_data: &mut Self) {
        match (self, other_data) {
            (NodeData::Branches(b1), NodeData::Branches(b2)) => {
                b1.append(b2);
                b1.sort_unstable_by_key(|b| b.key);
            }
            (NodeData::Leaves(l1), NodeData::Leaves(l2)) => {
                l1.append(l2);
                l1.sort_unstable_by_key(|l| l.key_parts());
            }
            _ => panic!("incompatible data types"),
        }
    }
}

pub(crate) struct Branch {
    key: SliceParts,
    pub(crate) page: PageID,
}

impl Branch {
    pub(crate) fn from_node(node: &Node) -> Branch {
        Branch {
            key: node.data.key_parts(),
            page: node.page_id,
        }
    }

    pub(crate) fn key(&self) -> &[u8] {
        self.key.slice()
    }

    pub(crate) fn key_size(&self) -> u64 {
        self.key.size()
    }
}

// Change to DataType
pub(crate) type NodeType = u8;

impl Node {
    pub(crate) const TYPE_DATA: NodeType = 0x00;
    pub(crate) const TYPE_BUCKET: NodeType = 0x01;

    pub(crate) fn new(id: NodeID, t: PageType, b: Ptr<BucketInner>) -> Node {
        let data: NodeData = match t {
            Page::TYPE_BRANCH => NodeData::Branches(Vec::new()),
            Page::TYPE_LEAF => NodeData::Leaves(Vec::new()),
            _ => panic!("INVALID PAGE TYPE FOR NEW NODE"),
        };
        Node {
            id,
            page_id: 0,
            num_pages: 0,
            bucket: b,
            children: Vec::new(),
            data,
            deleted: false,
            original_key: None,
        }
    }

    pub(crate) fn with_data(id: NodeID, data: NodeData, b: Ptr<BucketInner>) -> Node {
        let original_key = Some(data.key_parts());
        Node {
            id,
            page_id: 0,
            num_pages: 0,
            bucket: b,
            children: Vec::new(),
            data,
            deleted: false,
            original_key,
        }
    }

    pub(crate) fn from_page(id: NodeID, b: Ptr<BucketInner>, p: &Page) -> Node {
        let data: NodeData = match p.page_type {
            Page::TYPE_BRANCH => {
                let mut data = Vec::with_capacity(p.count as usize);
                for branch in p.branch_elements() {
                    data.push(Branch {
                        key: SliceParts::from_slice(branch.key()),
                        page: branch.page,
                    });
                }
                NodeData::Branches(data)
            }
            Page::TYPE_LEAF => {
                let mut data = Vec::with_capacity(p.count as usize);
                for leaf in p.leaf_elements() {
                    data.push(Data::from_leaf(leaf));
                }
                NodeData::Leaves(data)
            }
            _ => panic!("INVALID PAGE TYPE FOR FROM_PAGE"),
        };
        let original_key = if data.len() > 0 {
            Some(data.key_parts())
        } else {
            None
        };
        Node {
            id,
            page_id: p.id,
            num_pages: p.overflow + 1,
            bucket: b,
            children: Vec::new(),
            data,
            deleted: false,
            original_key,
        }
    }

    pub(crate) fn leaf(&self) -> bool {
        match &self.data {
            NodeData::Branches(_) => false,
            NodeData::Leaves(_) => true,
        }
    }

    pub(crate) fn insert_data(&mut self, data: Data) {
        match &mut self.data {
            NodeData::Branches(_) => panic!("CANNOT INSERT DATA INTO A BRANCH NODE"),
            NodeData::Leaves(leaves) => {
                match leaves.binary_search_by_key(&data.key(), |d| &d.key()) {
                    Ok(i) => leaves[i] = data,
                    Err(i) => leaves.insert(i, data),
                };
            }
        }
    }

    pub(crate) fn insert_child(&mut self, id: NodeID, key: SliceParts) {
        match &mut self.data {
            NodeData::Branches(branches) => {
                debug_assert!(!self.children.contains(&id));
                debug_assert!(branches
                    .binary_search_by_key(&key.slice(), |b| &b.key())
                    .is_ok());
                self.children.push(id);
            }
            NodeData::Leaves(_) => panic!("CANNOT INSERT BRANCH INTO A LEAF NODE"),
        }
    }

    pub(crate) fn delete(&mut self, index: usize) -> Data {
        match &mut self.data {
            NodeData::Branches(_) => panic!("CANNOT DELETE DATA FROM A BRANCH NODE"),
            NodeData::Leaves(leaves) => leaves.remove(index),
        }
    }

    fn size(&self) -> u64 {
        HEADER_SIZE + self.data.size()
    }

    pub(crate) fn write(&mut self, file: &mut File) -> Result<()> {
        if self.deleted {
            return Ok(());
        }
        let size = self.size();
        let mut buf: Vec<u8> = vec![0; size as usize];
        #[allow(clippy::cast_ptr_alignment)]
        let page = unsafe { &mut *(&mut buf[0] as *mut u8 as *mut Page) };
        page.write_node(self, self.num_pages)?;
        let offset = (self.page_id as u64) * (self.bucket.tx.meta.pagesize as u64);
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(buf.as_slice())?;
        Ok(())
    }

    pub(crate) fn merge(&mut self) -> bool {
        // merge children if it is a branch node
        if let NodeData::Branches(branches) = &mut self.data {
            let mut deleted_children = vec![];
            let mut i = 0;
            while i < self.children.len() {
                // stop if there is only one branch left
                if branches.len() == 1 {
                    break;
                }
                let mut b = Ptr::new(&self.bucket);
                let id = PageNodeID::Node(self.children[i]);
                let child = self.bucket.node(id);
                // check if child needs to be merged.
                if child.merge() {
                    // find the child's branch element in this node's data
                    let index = match branches
                        .binary_search_by_key(&child.original_key.unwrap().slice(), |b| b.key())
                    {
                        Ok(i) => i,
                        _ => panic!("THIS IS VERY VERY BAD"),
                    };
                    // check if there is any data left to copy
                    if child.data.len() > 0 {
                        // add that child's data to a sibling node
                        let sibling_page = if index == 0 {
                            // right sibling
                            branches[index + 1].page
                        } else {
                            // left sibling
                            branches[index - 1].page
                        };
                        let sibling = b.node(PageNodeID::Page(sibling_page));
                        sibling.data.merge(&mut child.data);
                    }

                    // free the child's page and mark it as deleted
                    child.free_page();
                    child.deleted = true;

                    // remove the child from this node
                    branches.remove(index);
                    deleted_children.push(i);
                }
                i += 1;
            }
            for c in deleted_children.iter().rev() {
                self.children.remove(*c);
            }
        }
        // determine if this node needs to be merged, and return the value
        // needs to be merged if it does not have enough keys, or if it doesn't fill 1/4 of a page
        self.data.len() < MIN_KEYS_PER_NODE || self.size() < (self.bucket.tx.db.pagesize / 4)
    }

    pub(crate) fn free_page(&mut self) {
        if self.page_id != 0 {
            self.bucket.tx.free(self.page_id, self.num_pages);
            self.page_id = 0;
        }
    }

    fn allocate(&mut self) {
        self.free_page();
        let size = self.size();
        let (page_id, num_pages) = self.bucket.tx.allocate(size);
        self.page_id = page_id;
        self.num_pages = num_pages;
    }

    pub(crate) fn split(&mut self) -> Option<Vec<Branch>> {
        // sort children so we iterate over them in order
        let mut b = Ptr::new(&self.bucket);
        self.children
            .sort_by_cached_key(|id| b.node(PageNodeID::Node(*id)).data.key_parts());
        for child in self.children.iter() {
            let child = self.bucket.node(PageNodeID::Node(*child));
            let new_branches = child.split();
            if let NodeData::Branches(branches) = &mut self.data {
                let index = match branches
                    .binary_search_by_key(&child.original_key.unwrap().slice(), |b| b.key())
                {
                    Ok(i) => i,
                    Err(i) => panic!(
                        "THIS IS VERY VERY BAD: {:?} {}",
                        &child.original_key.unwrap().slice(),
                        i
                    ),
                };
                branches[index] = Branch::from_node(&child);
                if let Some(mut new_branches) = new_branches {
                    let mut right_side = branches.split_off(index + 1);
                    branches.append(&mut new_branches);
                    branches.append(&mut right_side);
                }
            }
        }
        if self.data.len() <= (MIN_KEYS_PER_NODE * 2) || self.size() < self.bucket.tx.db.pagesize {
            self.allocate();
            return None;
        }
        let threshold = ((self.bucket.tx.db.pagesize as f32) * FILL_PERCENT) as u64;
        let mut split_indexes = Vec::<usize>::new();
        let mut current_size = HEADER_SIZE;
        let mut count = 0;
        match &self.data {
            NodeData::Branches(b) => {
                for (i, b) in b.iter().enumerate() {
                    count += 1;
                    let size = BRANCH_SIZE + b.key_size();
                    let new_size = current_size + size;
                    if count >= MIN_KEYS_PER_NODE && new_size > threshold {
                        split_indexes.push(i);
                        current_size = HEADER_SIZE + size;
                        count = 0;
                    } else {
                        current_size = new_size;
                    }
                }
            }
            NodeData::Leaves(leaves) => {
                for (i, l) in leaves.iter().enumerate() {
                    count += 1;
                    let size = LEAF_SIZE + l.size();
                    let new_size = current_size + size;
                    if count >= MIN_KEYS_PER_NODE && new_size > threshold {
                        split_indexes.push(i);
                        current_size = HEADER_SIZE + size;
                        count = 0;
                    } else {
                        current_size = new_size;
                    }
                }
            }
        };
        // for some reason we didn't find a place to split
        if split_indexes.is_empty() {
            self.allocate();
            return None;
        }

        let new_data: Vec<NodeData> = split_indexes
            .iter()
            // split from the end so we only break off small chunks at a time
            .rev()
            // split the data
            .map(|i| self.data.split_at(*i))
            .collect();
        self.allocate();

        Some(
            new_data
                .into_iter()
                .rev()
                .map(|data| {
                    let n = self.bucket.new_node(data);
                    n.allocate();
                    Branch::from_node(n)
                })
                .collect(),
        )
    }
}
