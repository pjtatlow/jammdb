use std::io::Write;
use std::mem::size_of;
use std::slice::{from_raw_parts, from_raw_parts_mut};

use crate::data::{BucketData, SliceParts};
use crate::errors::Result;
use crate::meta::Meta;
use crate::node::{Node, NodeData, NodeType};
use crate::transaction::TransactionInner;

pub(crate) type PageID = u64;

pub(crate) type PageType = u8;

#[repr(C)]
#[derive(Debug)]
pub(crate) struct Page {
    pub(crate) id: PageID,
    pub(crate) page_type: PageType,
    pub(crate) count: u64,
    pub(crate) overflow: u64,
    pub(crate) ptr: u64,
}

impl Page {
    pub(crate) const TYPE_BRANCH: PageType = 0x01;
    pub(crate) const TYPE_LEAF: PageType = 0x02;
    pub(crate) const TYPE_META: PageType = 0x03;
    pub(crate) const TYPE_FREELIST: PageType = 0x04;

    #[inline]
    pub(crate) fn from_buf(buf: &[u8], id: PageID, pagesize: u64) -> &Page {
        #[allow(clippy::cast_ptr_alignment)]
        unsafe {
            &*(&buf[(id * pagesize) as usize] as *const u8 as *const Page)
        }
    }

    pub(crate) fn meta(&self) -> &Meta {
        assert_eq!(self.page_type, Page::TYPE_META);
        unsafe { &*(&self.ptr as *const u64 as *const Meta) }
    }

    pub(crate) fn meta_mut(&mut self) -> &mut Meta {
        assert_eq!(self.page_type, Page::TYPE_META);
        unsafe { &mut *(&mut self.ptr as *mut u64 as *mut Meta) }
    }

    pub(crate) fn freelist(&self) -> &[PageID] {
        assert_eq!(self.page_type, Page::TYPE_FREELIST);
        unsafe {
            let start = &self.ptr as *const u64 as *const PageID;
            from_raw_parts(start, self.count as usize)
        }
    }

    pub(crate) fn freelist_mut(&mut self) -> &mut [PageID] {
        assert_eq!(self.page_type, Page::TYPE_FREELIST);
        unsafe {
            let start = &self.ptr as *const u64 as *mut PageID;
            from_raw_parts_mut(start, self.count as usize)
        }
    }

    pub(crate) fn leaf_elements(&self) -> &[LeafElement] {
        assert_eq!(self.page_type, Page::TYPE_LEAF);
        unsafe {
            let start = &self.ptr as *const u64 as *const LeafElement;
            from_raw_parts(start, self.count as usize)
        }
    }

    pub(crate) fn branch_elements(&self) -> &[BranchElement] {
        assert_eq!(self.page_type, Page::TYPE_BRANCH);
        unsafe {
            let start = &self.ptr as *const u64 as *const BranchElement;
            from_raw_parts(start, self.count as usize)
        }
    }

    pub(crate) fn leaf_elements_mut(&mut self) -> &mut [LeafElement] {
        assert_eq!(self.page_type, Page::TYPE_LEAF);
        unsafe {
            let start = &self.ptr as *const u64 as *const LeafElement as *mut LeafElement;
            from_raw_parts_mut(start, self.count as usize)
        }
    }

    pub(crate) fn branch_elements_mut(&mut self) -> &mut [BranchElement] {
        assert_eq!(self.page_type, Page::TYPE_BRANCH);
        unsafe {
            let start = &self.ptr as *const u64 as *const BranchElement as *mut BranchElement;
            from_raw_parts_mut(start, self.count as usize)
        }
    }

    fn slice(&mut self, size: u64) -> &mut [u8] {
        unsafe {
            let start = &self.ptr as *const u64 as *const u8 as *mut u8;
            from_raw_parts_mut(start, size as usize)
        }
    }

    pub(crate) fn write_node(&mut self, n: &Node, num_pages: u64) -> Result<()> {
        self.id = n.page_id;
        self.count = n.data.len() as u64;
        self.overflow = num_pages - 1;
        let header_size;
        let mut data_size: u64 = 0;
        let mut data: Vec<&[u8]>;
        match &n.data {
            NodeData::Branches(branches) => {
                self.page_type = Page::TYPE_BRANCH;
                header_size = size_of::<BranchElement>() as u64;
                let mut header_offsets = header_size * (branches.len() as u64);
                data = Vec::with_capacity(self.count as usize);
                let elems = self.branch_elements_mut();
                for (b, elem) in branches.iter().zip(elems.iter_mut()) {
                    debug_assert!(b.page != 0, "PAGE SHOULD NOT BE ZERO!");
                    elem.page = b.page;
                    elem.key_size = b.key_size();
                    elem.pos = header_offsets + data_size;
                    data_size += elem.key_size;
                    header_offsets -= header_size;
                    data.push(b.key());
                }
            }
            NodeData::Leaves(leaves) => {
                self.page_type = Page::TYPE_LEAF;
                header_size = size_of::<LeafElement>() as u64;
                let mut header_offsets = header_size * (leaves.len() as u64);
                data = Vec::with_capacity(self.count as usize * 2);
                let elems = self.leaf_elements_mut();
                for (l, elem) in leaves.iter().zip(elems.iter_mut()) {
                    elem.node_type = l.node_type();

                    let key = l.key();
                    let value = l.value();
                    elem.key_size = key.len() as u64;
                    elem.value_size = value.len() as u64;
                    elem.pos = header_offsets + data_size;

                    data_size += elem.key_size + elem.value_size;
                    header_offsets -= header_size;

                    data.push(key);
                    data.push(value);
                }
            }
        };
        let total_header = header_size * self.count;
        let buf = self.slice(total_header + data_size);
        let mut buf = &mut buf[(total_header as usize)..];
        for b in data.iter() {
            buf.write_all(b)?;
        }
        Ok(())
    }

    #[cfg_attr(tarpaulin, skip)]
    pub fn print(&self, tx: &TransactionInner) {
        let name = self.name();
        println!("{} [style=\"filled\", fillcolor=\"darkorchid1\"];", name);
        match self.page_type {
            Page::TYPE_BRANCH => {
                for (i, elem) in self.branch_elements().iter().enumerate() {
                    let key = elem.key();
                    let elem_name =
                        format!("\"Index: {}\\nPage: {}\\nKey: {:?}\"", i, self.id, key);
                    let page = tx.page(elem.page);
                    println!("{} [style=\"filled\", fillcolor=\"burlywood\"];", elem_name);
                    println!("{} -> {}", name, elem_name);
                    println!("{} -> {}", elem_name, page.name());
                    page.print(tx);
                }
            }
            Page::TYPE_LEAF => {
                for (i, elem) in self.leaf_elements().iter().enumerate() {
                    match elem.node_type {
                        Node::TYPE_BUCKET => {
                            // let parts = SliceParts::from_slice(elem.value());
                            let bd = BucketData::from_slice_parts(
                                SliceParts::from_slice(elem.key()),
                                SliceParts::from_slice(elem.value()),
                            );
                            let meta = bd.meta();
                            let elem_name = format!(
                                "\"Index: {}\\nPage: {}\\nKey {:?}\\n {:?}\"",
                                i,
                                self.id,
                                elem.key(),
                                meta
                            );
                            println!("{} [style=\"filled\", fillcolor=\"gray91\"];", elem_name);
                            println!("{} -> {}", name, elem_name);
                            let page = tx.page(meta.root_page);
                            println!("{} -> {}", elem_name, page.name());
                            page.print(tx);
                        }
                        Node::TYPE_DATA => {
                            // return;
                            let elem_name = format!(
                                "\"Index: {}\\nPage: {}\\nKey: {:?}\\nValue '{}'\"",
                                i,
                                self.id,
                                elem.key(),
                                std::str::from_utf8(elem.value()).unwrap()
                            );
                            // let elem_name = format!("\"Index: {}\\nPage: {}\\nKey: '{}'\\nValue '{}'\"", i, self.id, std::str::from_utf8(elem.key()).unwrap(), std::str::from_utf8(elem.value()).unwrap());
                            println!(
                                "{} [style=\"filled\", fillcolor=\"chartreuse\"];",
                                elem_name
                            );
                            println!("{} -> {}", name, elem_name);
                        }
                        _ => panic!("LOL NOPE"),
                    }
                }
            }
            _ => panic!(
                "CANNOT WRITE NODE OF TYPE {} from page {}",
                type_str(self.page_type),
                self.id
            ),
        }
    }

    #[cfg_attr(tarpaulin, skip)]
    pub fn name(&self) -> String {
        let size = 4096 + (self.overflow * 4096);
        format!(
            "\"Page #{} ({}) ({} bytes)\"",
            self.id,
            type_str(self.page_type),
            size
        )
    }
}

#[cfg_attr(tarpaulin, skip)]
fn type_str(pt: PageType) -> String {
    match pt {
        Page::TYPE_BRANCH => String::from("Branch"),
        Page::TYPE_FREELIST => String::from("FreeList"),
        Page::TYPE_LEAF => String::from("Leaf"),
        Page::TYPE_META => String::from("Meta"),
        _ => format!("Invalid ({:#X})", pt),
    }
}

#[repr(C)]
pub(crate) struct BranchElement {
    pub(crate) page: PageID,
    key_size: u64,
    pos: u64,
}

impl BranchElement {
    pub(crate) fn key(&self) -> &[u8] {
        let pos = self.pos as usize;
        unsafe {
            let start = self as *const BranchElement as *const u8;
            let buf = std::slice::from_raw_parts(start, pos + (self.key_size as usize));
            &buf[pos..]
        }
    }
}

#[repr(C)]
pub(crate) struct LeafElement {
    pub(crate) node_type: NodeType,
    pos: u64,
    key_size: u64,
    value_size: u64,
}

impl LeafElement {
    pub(crate) fn key(&self) -> &[u8] {
        let pos = self.pos as usize;
        unsafe {
            let start = self as *const LeafElement as *const u8;
            let buf = std::slice::from_raw_parts(start, pos + self.key_size as usize);
            &buf[pos..]
        }
    }
    pub(crate) fn value(&self) -> &[u8] {
        let pos = (self.pos + self.key_size) as usize;
        unsafe {
            let start = self as *const LeafElement as *const u8;
            let buf = std::slice::from_raw_parts(start, pos + self.value_size as usize);
            &buf[pos..]
        }
    }
}
