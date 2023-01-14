use std::{cell::RefCell, rc::Rc};

use crate::{
    node::{Leaf, Node, NodeData, NodeID},
    page::{Page, PageID},
};

#[derive(Clone, Copy)]
pub(crate) enum PageNodeID {
    Page(PageID),
    Node(NodeID),
}

pub(crate) enum PageNode<'a> {
    Page(&'a Page),
    Node(Rc<RefCell<Node<'a>>>),
}

impl<'a> PageNode<'a> {
    pub fn id(&self) -> PageNodeID {
        match self {
            PageNode::Page(p) => PageNodeID::Page(p.id),
            PageNode::Node(n) => PageNodeID::Node(n.borrow().id),
        }
    }
    pub fn leaf(&self) -> bool {
        match self {
            PageNode::Page(p) => p.page_type == Page::TYPE_LEAF,
            PageNode::Node(n) => n.borrow().leaf(),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            PageNode::Page(p) => p.count as usize,
            PageNode::Node(n) => n.borrow().data.len(),
        }
    }

    pub fn index_page(&self, index: usize) -> PageID {
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
                let n = n.borrow();
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

    pub fn index(&self, key: &[u8]) -> (usize, bool) {
        let result = match self {
            PageNode::Page(p) => match p.page_type {
                Page::TYPE_LEAF => p.leaf_elements().binary_search_by_key(&key, |e| e.key()),
                Page::TYPE_BRANCH => p.branch_elements().binary_search_by_key(&key, |e| e.key()),
                _ => panic!("INVALID PAGE TYPE FOR INDEX: {:?}", p.page_type),
            },
            PageNode::Node(n) => match &n.borrow().data {
                NodeData::Branches(b) => b.binary_search_by_key(&key, |b| b.key()),
                NodeData::Leaves(l) => l.binary_search_by_key(&key, |l| l.key()),
            },
        };
        match result {
            Ok(i) => (i, true),
            // we didn't find the element, so point at the element just "before" the missing element
            Err(mut i) => {
                i = i.saturating_sub(1);
                (i, false)
            }
        }
    }

    pub fn val<'b>(&'b self, index: usize) -> Option<Leaf<'a>> {
        match self {
            PageNode::Page(p) => match p.page_type {
                Page::TYPE_LEAF => p.leaf_elements().get(index).map(Leaf::from_leaf),
                _ => panic!("INVALID PAGE TYPE FOR VAL"),
            },
            PageNode::Node(n) => match &n.borrow().data {
                NodeData::Leaves(l) => l.get(index).cloned(),
                _ => panic!("INVALID NODE TYPE FOR VAL"),
            },
        }
    }
}
