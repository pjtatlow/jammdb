use std::{
    alloc::Layout,
    collections::{BTreeMap, BTreeSet},
    mem::size_of,
    ptr::NonNull,
};

use bumpalo::Bump;

use crate::{
    meta::Meta,
    page::{Page, PageID},
    Result,
};

pub(crate) struct TxFreelist {
    pub(crate) meta: Meta,
    pub(crate) inner: Freelist,
    pub(crate) pages: BTreeMap<u64, (NonNull<u8>, usize)>,
    pub(crate) arena: Bump,
}

impl<'a> TxFreelist {
    pub(crate) fn new(meta: Meta, inner: Freelist) -> TxFreelist {
        TxFreelist {
            meta,
            inner,
            pages: BTreeMap::new(),
            arena: Bump::new(),
        }
    }

    pub(crate) fn free(&mut self, page_id: PageID, num_pages: u64) {
        debug_assert!(num_pages > 0, "cannot free zero pages");
        for id in page_id..(page_id + num_pages) {
            self.inner.free(self.meta.tx_id, id);
        }
    }

    pub(crate) fn allocate<'b>(&'b mut self, bytes: u64) -> Result<&'a mut Page> {
        assert!(
            bytes >= (size_of::<Page>() as u64),
            "cannot allocate {} bytes, minimum is {}, {}",
            bytes,
            size_of::<Page>(),
            bytes < (size_of::<Page>() as u64)
        );
        let num_pages = if (bytes % self.meta.pagesize) == 0 {
            bytes / self.meta.pagesize
        } else {
            (bytes / self.meta.pagesize) + 1
        };
        let page_id = match self.inner.allocate(num_pages as usize) {
            Some(page_id) => page_id,
            None => {
                let page_id = self.meta.num_pages;
                self.meta.num_pages += num_pages;
                page_id
            }
        };

        let ptr = self
            .arena
            .alloc_layout(Layout::array::<u8>(bytes as usize)?.align_to(8)?);

        let page = unsafe { &mut *(ptr.as_ptr() as *mut Page) };
        page.id = page_id;
        page.overflow = num_pages - 1;
        self.pages.insert(page_id, (ptr, bytes as usize));

        Ok(page)
    }
}

#[derive(Clone)]
pub(crate) struct Freelist {
    free_pages: BTreeSet<PageID>,
    pending_pages: BTreeMap<u64, Vec<PageID>>,
}

const HEADER_SIZE: u64 = size_of::<Page>() as u64;
const PAGE_ID_SIZE: u64 = size_of::<PageID>() as u64;

impl Freelist {
    pub(crate) fn new() -> Freelist {
        Freelist {
            free_pages: BTreeSet::new(),
            pending_pages: BTreeMap::new(),
        }
    }

    pub(crate) fn init(&mut self, free_pages: &[PageID]) {
        free_pages.iter().for_each(|id| {
            self.free_pages.insert(*id);
        });
    }

    // adds the page to the transaction's set of free pages
    pub(crate) fn free(&mut self, tx_id: u64, page_id: PageID) {
        debug_assert!(
            page_id > 1,
            "cannot free page {}, reserved for meta",
            page_id
        );
        let pages = self.pending_pages.entry(tx_id).or_insert_with(Vec::new);
        pages.push(page_id);
    }

    // frees all pages from old transactions that have lower ids than the given tx_id
    pub(crate) fn release(&mut self, tx_id: u64) {
        let pending_ids: Vec<u64> = self.pending_pages.keys().cloned().collect();
        for other_tx_id in pending_ids {
            if other_tx_id < tx_id {
                let pages = self.pending_pages.remove(&other_tx_id).unwrap();
                pages.into_iter().for_each(|p| {
                    self.free_pages.insert(p);
                });
            } else {
                break;
            }
        }
    }

    pub(crate) fn allocate(&mut self, num_pages: usize) -> Option<PageID> {
        if self.free_pages.is_empty() {
            return None;
        }
        let mut start: PageID = 0;
        let mut prev: PageID = 0;
        let mut found: PageID = 0;

        for id in self.free_pages.iter().cloned() {
            debug_assert!(
                id > 1,
                "pageID {} cannot be in freelist, reserved for meta",
                id
            );

            if prev == 0 || id - prev != 1 {
                start = id;
            }

            let block_size = id - start + 1;
            if block_size == (num_pages as u64) {
                found = start;
                break;
            }

            prev = id;
        }

        if found > 0 {
            for id in found..found + (num_pages as u64) {
                self.free_pages.remove(&id);
            }
            return Some(found);
        }

        None
    }

    pub(crate) fn pages(&self) -> Vec<PageID> {
        let mut page_ids: Vec<PageID> = self.free_pages.iter().cloned().collect();
        for (_, pages) in self.pending_pages.iter() {
            let mut pages = pages.to_vec();
            page_ids.append(&mut pages);
        }
        page_ids.sort_unstable();
        page_ids
    }

    pub(crate) fn size(&self) -> u64 {
        let count = self.pages().len() as u64;
        HEADER_SIZE + (PAGE_ID_SIZE * count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{errors::Result, testutil::RandomFile, OpenOptions};

    fn freelist_from_vec(v: Vec<PageID>) -> Freelist {
        let mut freelist = Freelist {
            free_pages: v.iter().cloned().collect(),
            pending_pages: BTreeMap::new(),
        };
        freelist.init(v.as_slice());
        freelist
    }

    #[test]
    fn test_allocate() {
        let mut freelist = freelist_from_vec(vec![2, 4, 6, 8, 9, 10]);
        assert_eq!(freelist.allocate(4), None);
        assert_eq!(freelist.allocate(1), Some(2));
        assert_eq!(
            freelist.free_pages.iter().cloned().collect::<Vec<u64>>(),
            vec![4, 6, 8, 9, 10]
        );
        assert_eq!(freelist.allocate(1), Some(4));
        assert_eq!(
            freelist.free_pages.iter().cloned().collect::<Vec<u64>>(),
            vec![6, 8, 9, 10]
        );
        assert_eq!(freelist.allocate(3), Some(8));
        assert_eq!(
            freelist.free_pages.iter().cloned().collect::<Vec<u64>>(),
            vec![6]
        );
        assert_eq!(freelist.allocate(1), Some(6));
        assert_eq!(
            freelist.free_pages.iter().cloned().collect::<Vec<u64>>(),
            vec![]
        );
        assert_eq!(freelist.allocate(1), None);
    }

    #[test]
    fn test_free() {
        let mut freelist = Freelist::new();

        freelist.free(1, 5);
        assert_eq!(freelist.pending_pages.len(), 1);
        assert_eq!(freelist.pending_pages.get(&1), Some(&vec![5]));
        freelist.free(1, 4);
        assert_eq!(freelist.pending_pages.len(), 1);
        assert_eq!(freelist.pending_pages.get(&1), Some(&vec![5, 4]));
        freelist.free(1, 3);
        assert_eq!(freelist.pending_pages.len(), 1);
        assert_eq!(freelist.pending_pages.get(&1), Some(&vec![5, 4, 3]));
        freelist.free(2, 7);
        assert_eq!(freelist.pending_pages.len(), 2);
        assert_eq!(freelist.pending_pages.get(&1), Some(&vec![5, 4, 3]));
        assert_eq!(freelist.pending_pages.get(&2), Some(&vec![7]));
        assert_eq!(freelist.free_pages, BTreeSet::new());
    }

    #[test]
    fn test_pages() {
        let mut freelist = freelist_from_vec(vec![1, 2, 3, 4, 5]);

        assert_eq!(freelist.pages(), vec![1, 2, 3, 4, 5]);
        freelist.free(2, 9);
        freelist.free(2, 10);
        freelist.free(2, 11);
        assert_eq!(freelist.pages(), vec![1, 2, 3, 4, 5, 9, 10, 11]);

        freelist.free(1, 6);
        freelist.free(1, 7);
        freelist.free(1, 8);
        assert_eq!(freelist.pages(), vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
    }

    #[test]
    fn test_release() {
        let mut freelist = Freelist::new();

        freelist.free(1, 5);
        freelist.free(1, 10);
        freelist.free(1, 7);

        assert_eq!(freelist.free_pages.len(), 0);
        assert_eq!(freelist.pending_pages.len(), 1);
        freelist.release(1);
        assert_eq!(freelist.free_pages.len(), 0);
        assert_eq!(freelist.pending_pages.len(), 1);

        freelist.release(2);
        assert_eq!(freelist.free_pages.len(), 3);
        assert_eq!(freelist.pending_pages.len(), 0);
        assert_eq!(freelist.pages(), vec![5, 7, 10]);
    }

    #[test]
    fn test_size() {
        let freelist = freelist_from_vec(vec![1, 2, 3]);
        assert_eq!(freelist.size(), HEADER_SIZE + (PAGE_ID_SIZE * 3));
    }

    #[test]
    fn test_allocate_no_freelist() -> Result<()> {
        let random_file = RandomFile::new();
        let db = OpenOptions::new()
            .pagesize(1024)
            .num_pages(4)
            .open(&random_file)?;
        let tx = db.tx(false)?;
        let tx = tx.inner.borrow_mut();
        let mut freelist = tx.freelist.borrow_mut();
        // make sure we have an empty freelist and only four pages
        assert_eq!(freelist.inner.pages().len(), 0);
        assert_eq!(tx.meta.num_pages, 4);
        // allocate one page worth of bytes
        let page = freelist.allocate(1024)?;
        assert!(page.id == 4);
        assert!(page.overflow == 0);
        // allocate a half page worth of bytes
        let page = freelist.allocate(512)?;
        assert!(page.id == 5);
        assert!(page.overflow == 0);

        // allocate ten pages worth of bytes
        let page = freelist.allocate(10240)?;
        assert!(page.id == 6);
        assert!(page.overflow == 9);

        // allocate a non pagesize number of bytes
        let page = freelist.allocate(1234)?;
        assert!(page.id == 16);
        assert!(page.overflow == 1);

        Ok(())
    }

    #[test]
    fn test_allocate_freelist() -> Result<()> {
        let random_file = RandomFile::new();
        let db = OpenOptions::new()
            .pagesize(1024)
            .num_pages(100)
            .open(&random_file)?;
        let tx = db.tx(false)?;
        let tx = tx.inner.borrow_mut();
        let mut freelist = tx.freelist.borrow_mut();

        // setup the freelist and num_pages to simulate a used database
        for page in [10_u64, 11, 13, 14, 15].iter() {
            freelist.free(*page, 1);
        }
        freelist.inner.release(1);
        freelist.meta.num_pages = 99;

        // allocate one page worth of bytes (should come from freelist)
        let page = freelist.allocate(1024)?;
        assert!(page.id == 10);
        assert!(page.overflow == 0);
        // allocate a half page worth of bytes (should come from freelist)
        let page = freelist.allocate(512)?;
        assert!(page.id == 11);
        assert!(page.overflow == 0);

        // allocate three-ish pages worth of bytes (should come from freelist)
        let page = freelist.allocate(3000)?;
        assert!(page.id == 13);
        assert!(page.overflow == 2);

        // allocate a small number of bytes
        let page = freelist.allocate(100)?;
        assert!(page.id == 99);
        assert!(page.overflow == 0);
        Ok(())
    }

    #[test]
    fn test_tx_free() -> Result<()> {
        let random_file = RandomFile::new();
        let db = OpenOptions::new()
            .pagesize(1024)
            .num_pages(100)
            .open(&random_file)?;
        let tx = db.tx(false)?;
        let tx = tx.inner.borrow_mut();
        let mut freelist = tx.freelist.borrow_mut();

        assert_eq!(tx.meta.tx_id, 0);
        assert_eq!(freelist.inner.pages().len(), 0);
        freelist.free(80, 1);
        assert_eq!(freelist.inner.pages(), vec![80]);
        freelist.free(100, 6);
        assert_eq!(
            freelist.inner.pages(),
            vec![80, 100, 101, 102, 103, 104, 105]
        );

        Ok(())
    }
}
