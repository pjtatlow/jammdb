use std::fs::File;
use std::io::{Seek, SeekFrom, Write};
use std::mem::size_of;

use crate::bucket::InnerBucket;
use crate::bytes::Bytes;
use crate::data::Data;
use crate::errors::Result;

use crate::freelist::TxFreelist;
use crate::page::{BranchElement, LeafElement, Page, PageID, PageType};

pub(crate) type NodeID = u64;

const HEADER_SIZE: u64 = size_of::<Page>() as u64;
const LEAF_SIZE: u64 = size_of::<LeafElement>() as u64;
const BRANCH_SIZE: u64 = size_of::<BranchElement>() as u64;
const MIN_KEYS_PER_NODE: usize = 2;
const FILL_PERCENT: f32 = 0.5;

pub(crate) struct Node<'n> {
    pub(crate) id: NodeID,
    pub(crate) page_id: PageID,
    pub(crate) num_pages: u64,

    // pub (crate) key: SliceParts,
    pub(crate) children: Vec<NodeID>,
    pub(crate) data: NodeData<'n>,
    pub(crate) deleted: bool,
    pub(crate) original_key: Option<Bytes<'n>>,
    pagesize: u64,
}

impl<'n> Node<'n> {
    pub(crate) fn new(id: NodeID, t: PageType, pagesize: u64) -> Node<'n> {
        let data: NodeData = match t {
            Page::TYPE_BRANCH => NodeData::Branches(Vec::new()),
            Page::TYPE_LEAF => NodeData::Leaves(Vec::new()),
            _ => panic!("INVALID PAGE TYPE FOR NEW NODE"),
        };
        Node {
            id,
            page_id: 0,
            num_pages: 0,
            children: Vec::new(),
            data,
            deleted: false,
            original_key: None,
            pagesize,
        }
    }

    pub(crate) fn from_page(id: NodeID, p: &Page, pagesize: u64) -> Node<'n> {
        let data: NodeData = match p.page_type {
            Page::TYPE_BRANCH => {
                let mut data = Vec::with_capacity(p.count as usize);
                for branch in p.branch_elements() {
                    data.push(Branch {
                        key: Bytes::Slice(branch.key()),
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
            // bucket: b,
            children: Vec::new(),
            data,
            deleted: false,
            original_key,
            pagesize,
        }
    }

    pub(crate) fn with_data(id: NodeID, data: NodeData<'n>, pagesize: u64) -> Node<'n> {
        let original_key = Some(data.key_parts());
        Node {
            id,
            page_id: 0,
            num_pages: 0,
            children: Vec::new(),
            data,
            deleted: false,
            original_key,
            pagesize,
        }
    }

    pub(crate) fn insert_child(&mut self, id: NodeID, key: Bytes) {
        match &mut self.data {
            NodeData::Branches(branches) => {
                debug_assert!(!self.children.contains(&id));
                debug_assert!(branches
                    .binary_search_by_key(&key.as_ref(), |b| b.key())
                    .is_ok());
                self.children.push(id);
            }
            NodeData::Leaves(_) => panic!("CANNOT INSERT BRANCH INTO A LEAF NODE"),
        }
    }

    pub(crate) fn insert_data<'a>(&'a mut self, data: Data<'n>) {
        match &mut self.data {
            NodeData::Branches(_) => panic!("CANNOT INSERT DATA INTO A BRANCH NODE"),
            NodeData::Leaves(leaves) => {
                match leaves.binary_search_by_key(&data.key(), |d| d.key()) {
                    Ok(i) => leaves[i] = data,
                    Err(i) => leaves.insert(i, data),
                };
            }
        }
    }

    pub(crate) fn delete<'a>(&'a mut self, index: usize) -> Data<'n> {
        match &mut self.data {
            NodeData::Branches(_) => panic!("CANNOT DELETE DATA FROM A BRANCH NODE"),
            NodeData::Leaves(leaves) => leaves.remove(index),
        }
    }

    pub(crate) fn leaf(&self) -> bool {
        match &self.data {
            NodeData::Branches(_) => false,
            NodeData::Leaves(_) => true,
        }
    }

    fn size(&self) -> u64 {
        HEADER_SIZE + self.data.size()
    }

    pub(crate) fn write(&self, file: &mut File) -> Result<()> {
        if self.deleted {
            return Ok(());
        }
        let size = self.size();
        let mut buf: Vec<u8> = vec![0; size as usize];
        #[allow(clippy::cast_ptr_alignment)]
        let page = unsafe { &mut *(&mut buf[0] as *mut u8 as *mut Page) };
        page.write_node(self, self.num_pages)?;
        let offset = (self.page_id as u64) * (self.pagesize as u64);
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(buf.as_slice())?;
        Ok(())
    }

    pub(crate) fn needs_merging(&self) -> bool {
        self.data.len() < MIN_KEYS_PER_NODE || self.size() < (self.pagesize / 4)
    }

    pub(crate) fn split<'a>(
        &'a mut self,
        bucket: &'a mut InnerBucket<'n>,
        tx_freelist: &'a mut TxFreelist,
    ) -> Option<Vec<Branch<'n>>> {
        // sort children so we iterate over them in order
        self.children
            .sort_by_cached_key(|id| bucket.nodes[*id as usize].borrow().data.key_parts());
        for child in self.children.iter() {
            let child = bucket.nodes[*child as usize].clone();
            let mut child = child.borrow_mut();
            let new_branches = child.split(bucket, tx_freelist);
            if let NodeData::Branches(branches) = &mut self.data {
                let index = match branches
                    .binary_search_by_key(&child.original_key.clone().unwrap().as_ref(), |b| {
                        b.key()
                    }) {
                    Ok(i) => i,
                    Err(i) => panic!(
                        "child node not found: {}",
                        // &child.original_key.as_ref().unwrap(),
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
        if self.data.len() <= (MIN_KEYS_PER_NODE * 2) || self.size() < self.pagesize {
            self.allocate(tx_freelist);
            return None;
        }
        let threshold = ((self.pagesize as f32) * FILL_PERCENT) as u64;
        let mut split_indexes = Vec::<usize>::new();
        let mut current_size = HEADER_SIZE;
        let mut count = 0;
        match &self.data {
            NodeData::Branches(b) => {
                for (i, b) in b.iter().enumerate() {
                    count += 1;
                    let size = BRANCH_SIZE + (b.key_size() as u64);
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
                    let size = LEAF_SIZE + (l.size() as u64);
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
            self.allocate(tx_freelist);
            return None;
        }

        // split all of the data on the split indexes
        // Create new vector of data to go on it's own pages
        #[allow(clippy::needless_collect)]
        let new_data: Vec<NodeData> = split_indexes
            .iter()
            // split from the end so we only break off small chunks at a time
            .rev()
            // split the data
            .map(|i| self.data.split_at(*i))
            .collect();
        // allocate a page to hold this node now that the data bas been split
        self.allocate(tx_freelist);

        // create nodes for each bit of data we split apart
        Some(
            new_data
                .into_iter()
                .rev()
                .map(|data| {
                    let n = bucket.new_node(data);
                    let mut n = n.borrow_mut();
                    n.allocate(tx_freelist);
                    Branch::from_node(&n)
                })
                .collect(),
        )
    }

    pub(crate) fn free_page(&mut self, tx_freelist: &mut TxFreelist) {
        if self.page_id != 0 {
            tx_freelist.free(self.page_id, self.num_pages);
            self.page_id = 0;
        }
    }

    fn allocate<'a>(&'a mut self, tx_freelist: &'a mut TxFreelist) {
        self.free_page(tx_freelist);
        let size = self.size();
        let (page_id, num_pages) = tx_freelist.allocate(size);
        self.page_id = page_id;
        self.num_pages = num_pages;
    }
}

pub(crate) enum NodeData<'a> {
    Branches(Vec<Branch<'a>>),
    Leaves(Vec<Data<'a>>),
}

impl<'a> NodeData<'a> {
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
                .fold(HEADER_SIZE + (BRANCH_SIZE * b.len() as u64), |acc, b| {
                    acc + b.key_size() as u64
                }),
            NodeData::Leaves(l) => l
                .iter()
                .fold(HEADER_SIZE + (LEAF_SIZE * l.len() as u64), |acc, l| {
                    acc + l.size() as u64
                }),
        }
    }

    pub(crate) fn key_parts<'b>(&'b self) -> Bytes<'a> {
        debug_assert!(self.len() > 0, "Cannot get key parts of empty data");
        match self {
            NodeData::Branches(b) => b.first().map(|b| b.key.clone()),
            NodeData::Leaves(l) => l.first().map(|l| l.key_parts()),
        }
        .unwrap()
    }

    pub(crate) fn merge(&mut self, other_data: &mut Self) {
        match (self, other_data) {
            (NodeData::Branches(b1), NodeData::Branches(b2)) => {
                b1.append(b2);
                b1.sort_unstable_by_key(|b| b.key.clone());
            }
            (NodeData::Leaves(l1), NodeData::Leaves(l2)) => {
                l1.append(l2);
                l1.sort_unstable_by_key(|l| l.key_parts());
            }
            _ => panic!("incompatible data types"),
        }
    }

    fn split_at<'b>(&'b mut self, index: usize) -> NodeData<'a> {
        match self {
            NodeData::Branches(b) => NodeData::Branches(b.split_off(index)),
            NodeData::Leaves(l) => NodeData::Leaves(l.split_off(index)),
        }
    }
}

pub(crate) struct Branch<'a> {
    key: Bytes<'a>,
    pub(crate) page: PageID,
}

impl<'a> Branch<'a> {
    pub(crate) fn from_node<'b>(node: &'b Node<'a>) -> Branch<'a> {
        Branch {
            key: node.data.key_parts(),
            page: node.page_id,
        }
    }

    pub(crate) fn key(&self) -> &[u8] {
        self.key.as_ref()
    }

    pub(crate) fn key_size(&self) -> usize {
        self.key.size()
    }
}

// Change to DataType
pub(crate) type NodeType = u8;

impl<'n> Node<'n> {
    pub(crate) const TYPE_DATA: NodeType = 0x00;
    pub(crate) const TYPE_BUCKET: NodeType = 0x01;
}
