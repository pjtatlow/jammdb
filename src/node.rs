use std::{cell::RefCell, mem::size_of, rc::Rc};

use crate::{
    bucket::{BucketMeta, InnerBucket, META_SIZE},
    bytes::Bytes,
    errors::Result,
    freelist::TxFreelist,
    page::{BranchElement, LeafElement, Page, PageID, PageType},
};

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
    pub(crate) children: Vec<NodeID>,
    pub(crate) data: NodeData<'n>,
    pub(crate) deleted: bool,
    pub(crate) original_key: Option<Bytes<'n>>,
    pub(crate) parent: Option<u64>,
    pagesize: u64,
    spilled: bool,
}

impl<'n> Node<'n> {
    // This is only used when creating a root node for a new bucket
    // So the parent is always going to be None
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
            spilled: false,
            parent: None,
        }
    }

    // This is used to initialize nodes for pages that are being modified.
    // The parent value needs to be set afterwards!
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
                    data.push(Leaf::from_leaf(leaf));
                }
                NodeData::Leaves(data)
            }
            _ => panic!("INVALID PAGE TYPE FOR FROM_PAGE"),
        };
        let original_key = if data.len() > 0 {
            Some(data.first_key())
        } else {
            None
        };
        Node {
            id,
            page_id: p.id,
            num_pages: p.overflow + 1,
            children: Vec::new(),
            data,
            deleted: false,
            original_key,
            pagesize,
            spilled: false,
            parent: None,
        }
    }

    // This is used to create new nodes created by splitting existing nodes.
    // They don't need to have their parent set since we no longer care about parent/child
    // relationships once we're splitting.
    pub(crate) fn with_data(id: NodeID, data: NodeData<'n>, pagesize: u64) -> Node<'n> {
        let original_key = Some(data.first_key());
        Node {
            id,
            page_id: 0,
            num_pages: 0,
            children: Vec::new(),
            data,
            deleted: false,
            original_key,
            pagesize,
            spilled: false,
            parent: None,
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

    pub(crate) fn insert_data<'a>(&'a mut self, leaf: Leaf<'n>) {
        match &mut self.data {
            NodeData::Branches(_) => panic!("CANNOT INSERT DATA INTO A BRANCH NODE"),
            NodeData::Leaves(leaves) => {
                match leaves.binary_search_by_key(&leaf.key(), |l| l.key()) {
                    Ok(i) => leaves[i] = leaf,
                    Err(i) => leaves.insert(i, leaf),
                };
            }
        }
    }

    pub(crate) fn insert_branch<'a>(
        &'a mut self,
        original_key: &Option<Bytes<'n>>,
        branch: Branch<'n>,
    ) {
        let search_key = match original_key {
            Some(k) => k.as_ref(),
            None => branch.key(),
        };
        match &mut self.data {
            NodeData::Leaves(_) => panic!("CANNOT INSERT BRANCH INTO A LEAF NODE"),
            NodeData::Branches(branches) => {
                match branches.binary_search_by_key(&search_key, |b| b.key()) {
                    Ok(i) => {
                        assert!(original_key.is_some());
                        branches[i] = branch
                    }
                    Err(i) => {
                        assert!(original_key.is_none());
                        branches.insert(i, branch)
                    }
                };
            }
        }
    }

    pub(crate) fn delete<'a>(&'a mut self, index: usize) -> Leaf<'n> {
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

    pub(crate) fn needs_merging(&self) -> bool {
        self.data.len() < MIN_KEYS_PER_NODE || self.size() < (self.pagesize / 4)
    }

    pub(crate) fn spill<'a>(
        &'a mut self,
        bucket: &'a mut InnerBucket<'n>,
        tx_freelist: &'a mut TxFreelist,
        parent: Option<&'a mut Self>,
    ) -> Result<Option<PageID>> {
        let mut root_page_id: Option<PageID> = None;
        if self.spilled {
            return Ok(root_page_id);
        }
        // Sort the children so we iterate over them in order
        self.children
            .sort_by_cached_key(|id| bucket.nodes[*id as usize].borrow().data.first_key());

        // spill all of the children nodes
        let mut i = 0_usize;
        // Becuase spilling a child can result in that child being split into more children
        // we iterate using an index checking the length of the children vector at each iteration.
        while i < self.children.len() {
            let child_id = self.children[i];
            let child = bucket.nodes[child_id as usize].clone();
            let mut child = child.borrow_mut();
            child.spill(bucket, tx_freelist, Some(self))?;
            i += 1;
        }

        let new_siblings = self.split(bucket);
        // We now have this node's final data, so write it to some dirty pages.
        self.write(tx_freelist)?;
        if let Some(new_siblings) = &new_siblings {
            // We have some new siblings to welcome into the world!
            // Get all of them spilled onto some dirty pages.
            self.write(tx_freelist)?;
            for s in new_siblings.iter() {
                let mut s = s.borrow_mut();
                s.write(tx_freelist)?;
            }
        }
        // Check if we have a parent...
        match parent {
            Some(parent) => {
                // If we do, update all of it's branches!
                // Note that this means self is not the root node and we will return None.
                // Tell our parent about our new page_id and key.

                parent.insert_branch(&self.original_key, Branch::from_node(self));
                if let Some(new_siblings) = new_siblings {
                    // Tell the parent about our new siblings
                    for s in new_siblings.iter() {
                        let s = s.borrow();
                        // All of these nodes are new, so the key will always be the first key in the dataset
                        let branch = Branch::from_node(&s);
                        parent.insert_branch(&None, branch);
                    }
                }
            }
            None => {
                // If we don't, we are currently the root node.
                match new_siblings {
                    // If we're currently the root node but we just spawned siblings,
                    // Then create a new root node to be our parent.
                    Some(new_siblings) => {
                        // Create branches for all of the children (ourselves included as the first child)
                        let mut branches: Vec<Branch> = Vec::with_capacity(new_siblings.len() + 1);
                        branches.push(Branch::from_node(self));
                        for s in new_siblings {
                            let s = s.borrow();
                            branches.push(Branch::from_node(&s));
                        }
                        // Create parent from those branches
                        let new_parent = bucket.new_node(NodeData::Branches(branches));
                        let mut new_parent = new_parent.borrow_mut();
                        // Spill the parent, potentially splitting it, and writing it's data to dirty pages
                        match new_parent.spill(bucket, tx_freelist, None)? {
                            // The new parent must return a new page_id, so we can update the bucket's
                            // root page.
                            Some(page_id) => root_page_id = Some(page_id),
                            None => panic!("New parent did not return a new root_page_id"),
                        };
                    }

                    None => {
                        // No siblings means that self is still the root node.
                        // Set the root_page_id to our new page.
                        root_page_id = Some(self.page_id);
                    }
                }
            }
        }

        Ok(root_page_id)
    }

    pub(crate) fn split<'a>(
        &'a mut self,
        bucket: &'a mut InnerBucket<'n>,
    ) -> Option<Vec<Rc<RefCell<Node<'n>>>>> {
        if self.data.len() <= (MIN_KEYS_PER_NODE * 2) || self.size() < self.pagesize {
            return None;
        }
        let threshold = ((self.pagesize as f32) * FILL_PERCENT) as u64;
        let mut split_indexes = Vec::<usize>::new();
        let mut current_size = HEADER_SIZE;
        let mut count = 0;
        match &self.data {
            NodeData::Branches(b) => {
                let len = b.len();
                for (i, b) in b[..len - 2].iter().enumerate() {
                    if i > len - 3 {
                        break;
                    }
                    count += 1;
                    let size = BRANCH_SIZE + (b.key_size() as u64);
                    let new_size = current_size + size;
                    if count >= MIN_KEYS_PER_NODE && new_size > threshold {
                        split_indexes.push(i + 1);
                        current_size = HEADER_SIZE + size;
                        count = 0;
                    } else {
                        current_size = new_size;
                    }
                }
            }
            NodeData::Leaves(leaves) => {
                let len = leaves.len();
                for (i, l) in leaves[..len - 2].iter().enumerate() {
                    // if i > len - 3 {
                    //     break;
                    // }
                    count += 1;
                    let size = LEAF_SIZE + (l.size() as u64);
                    let new_size = current_size + size;
                    if count >= MIN_KEYS_PER_NODE && new_size > threshold {
                        split_indexes.push(i + 1);
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
            return None;
        }

        // split all of the data on the split indexes
        // Create new vector of data to go on it's own pages
        #[allow(clippy::needless_collect)]
        let new_data: Vec<NodeData> = split_indexes
            .into_iter()
            // Split from the right size so we only break off small chunks at a time.
            .rev()
            // Split the data.
            .map(|i| self.data.split_at(i))
            .collect();

        // Create nodes for each bit of data we split apart.
        Some(
            new_data
                .into_iter()
                // Reverse again so the nodes are in the correct order
                .rev()
                .map(|data| bucket.new_node(data))
                .collect(),
        )
    }

    // Write this node to a new (in-memory) page.
    pub(crate) fn write(&mut self, tx_freelist: &mut TxFreelist) -> Result<()> {
        if self.deleted {
            return Ok(());
        }
        self.spilled = true;
        let page = self.allocate(tx_freelist);
        page.write_node(self, self.num_pages)
    }

    // Free our old page (if we have one) and get a new page for ourselves.
    fn allocate<'a>(&'a mut self, tx_freelist: &'a mut TxFreelist) -> &'n mut Page {
        self.free_page(tx_freelist);
        let size = self.size();
        let page = tx_freelist.allocate(size);
        self.page_id = page.id;
        self.num_pages = page.overflow + 1;
        page
    }

    // Give our current page back to the freelist (if we have one)
    pub(crate) fn free_page(&mut self, tx_freelist: &mut TxFreelist) {
        if self.page_id != 0 {
            tx_freelist.free(self.page_id, self.num_pages);
            self.page_id = 0;
        }
    }
}

pub(crate) enum NodeData<'a> {
    Branches(Vec<Branch<'a>>),
    Leaves(Vec<Leaf<'a>>),
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
            NodeData::Branches(b) => b.iter().fold(BRANCH_SIZE * b.len() as u64, |acc, b| {
                acc + b.key_size() as u64
            }),
            NodeData::Leaves(l) => l
                .iter()
                .fold(LEAF_SIZE * l.len() as u64, |acc, l| acc + l.size() as u64),
        }
    }

    pub(crate) fn first_key<'b>(&'b self) -> Bytes<'a> {
        debug_assert!(self.len() > 0, "Cannot get key parts of empty data");
        match self {
            NodeData::Branches(b) => b[0].key.clone(),
            NodeData::Leaves(l) => l[0].key_bytes(),
        }
    }

    pub(crate) fn merge(&mut self, other_data: &mut Self) {
        match (self, other_data) {
            (NodeData::Branches(b1), NodeData::Branches(b2)) => {
                b1.append(b2);
                b1.sort_unstable_by_key(|b| b.key.clone());
            }
            (NodeData::Leaves(l1), NodeData::Leaves(l2)) => {
                l1.append(l2);
                l1.sort_unstable_by_key(|l| l.key_bytes());
                let mut last = l1[0].key();
                for l in l1[1..].iter() {
                    if last >= l.key() {
                        println!("HA. GOT 'EM!");
                    }
                    last = l.key();
                }
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
            key: node.data.first_key(),
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

#[derive(Clone)]
pub(crate) enum Leaf<'a> {
    Bucket(Bytes<'a>, BucketMeta),
    Kv(Bytes<'a>, Bytes<'a>),
}

impl<'a> Leaf<'a> {
    pub(crate) fn from_leaf<'b>(l: &'b LeafElement) -> Leaf<'a> {
        match l.node_type {
            Node::TYPE_DATA => Leaf::Kv(Bytes::Slice(l.key()), Bytes::Slice(l.value())),
            Node::TYPE_BUCKET => Leaf::Bucket(Bytes::Slice(l.key()), l.value().into()),
            _ => panic!("INVALID NODE TYPE"),
        }
    }

    pub(crate) fn node_type(&self) -> NodeType {
        match self {
            Self::Bucket(_, _) => Node::TYPE_BUCKET,
            Self::Kv(_, _) => Node::TYPE_DATA,
        }
    }

    pub(crate) fn key_bytes<'b>(&'b self) -> Bytes<'a> {
        match self {
            Self::Bucket(name, _) => name.clone(),
            Self::Kv(k, _) => k.clone(),
        }
    }

    pub(crate) fn key(&self) -> &[u8] {
        match self {
            Self::Bucket(b, _) => b.as_ref(),
            Self::Kv(k, _) => k.as_ref(),
        }
    }

    pub(crate) fn value(&self) -> &[u8] {
        match self {
            Self::Bucket(_, meta) => meta.as_ref(),
            Self::Kv(_, v) => v.as_ref(),
        }
    }

    pub(crate) fn size(&self) -> usize {
        match self {
            Self::Bucket(b, _) => b.size() + META_SIZE,
            Self::Kv(k, v) => k.size() + v.size(),
        }
    }

    pub(crate) fn is_kv(&self) -> bool {
        match self {
            Self::Bucket(_, _) => false,
            Self::Kv(_, _) => true,
        }
    }
}

// Change to DataType
pub(crate) type NodeType = u8;

impl<'n> Node<'n> {
    pub(crate) const TYPE_DATA: NodeType = 0x00;
    pub(crate) const TYPE_BUCKET: NodeType = 0x01;
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use super::*;
    use crate::{
        testutil::{rand_bytes, RandomFile},
        OpenOptions,
    };

    #[test]
    fn test_split() -> Result<()> {
        let random_file = RandomFile::new();
        let db = OpenOptions::new().pagesize(1024).open(&random_file)?;
        // Test split
        {
            let tx = db.tx(true)?;
            let b = tx.create_bucket("a")?;
            let mut data = HashMap::new();
            // Insert six nodes, each the size of a page.
            for key in ["a", "b", "c", "d", "e", "f"] {
                let value = rand_bytes(512);
                b.put(key, value.clone())?;
                data.insert(key, value);
            }
            {
                // Since this bucket was just created, there should be one node.
                let mut b = b.inner.borrow_mut();
                assert!(b.nodes.len() == 1);

                let tx_freelist = tx.inner.borrow().freelist.clone();
                let mut tx_freelist = tx_freelist.borrow_mut();
                b.spill(&mut tx_freelist)?;
                // Since everything is spilled, there should be two key / value pairs to a list.
                // That means we should have three leaf nodes and one branch node at the root.
                assert!(b.nodes.len() == 4);
                // Make sure the branch has the right data
                let branch_node = &b.nodes[3];
                let branch_node = branch_node.borrow();
                if let NodeData::Branches(branches) = &branch_node.data {
                    assert!(branches.len() == 3);

                    assert!(branches[0].key() == b"a");
                    assert!(branches[0].page == 7);

                    assert!(branches[1].key() == b"c");
                    assert!(branches[1].page == 10);

                    assert!(branches[2].key() == b"e");
                    assert!(branches[2].page == 13);
                } else {
                    panic!("Node 3 should have been a branch node")
                }
                // Make sure each node has the right data
                for (n, keys) in b.nodes[0..=2]
                    .iter()
                    .zip([["a", "b"], ["c", "d"], ["e", "f"]])
                {
                    let n = n.borrow();
                    assert!(n.data.len() == 2);
                    match &n.data {
                        NodeData::Leaves(leaves) => {
                            for (kv, key) in leaves.iter().zip(keys) {
                                assert!(kv.key() == key.as_bytes());
                                assert!(kv.value() == data[key]);
                            }
                        }
                        _ => panic!("Must be a leaf node"),
                    }
                }
            }
        }
        Ok(())
    }
}
