use fnv::FnvHasher;
use std::hash::Hasher;
use crate::{bucket::BucketMeta, page::PageID};

#[repr(C)]
#[derive(Debug, Clone)]
pub(crate) struct Meta {
    pub(crate) meta_page: u32,
    pub(crate) magic: u32,
    pub(crate) version: u32,
    pub(crate) pagesize: u64,
    pub(crate) root: BucketMeta,
    pub(crate) num_pages: PageID,
    pub(crate) freelist_page: PageID,
    pub(crate) tx_id: u64,
    pub(crate) hash: u64,
}

impl Meta {
    pub(crate) fn valid(&self) -> bool {
        self.hash == self.hash_self()
    }

    pub(crate) fn hash_self(&self) -> u64 {
        let mut hasher = FnvHasher::default();

        hasher.write(&self.meta_page.to_be_bytes());
        hasher.write(&self.magic.to_be_bytes());
        hasher.write(&self.version.to_be_bytes());
        hasher.write(&self.pagesize.to_be_bytes());
        hasher.write(&self.root.root_page.to_be_bytes());
        hasher.write(&self.root.next_int.to_be_bytes());
        hasher.write(&self.num_pages.to_be_bytes());
        hasher.write(&self.freelist_page.to_be_bytes());
        hasher.write(&self.tx_id.to_be_bytes());

        hasher.finish()
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_meta() {
        let mut meta = Meta {
            meta_page: 1,
            magic: 1_234_567_890,
            version: 987_654_321,
            pagesize: 4096,
            root: BucketMeta {
                root_page: 2,
                next_int: 2020,
            },
            num_pages: 13,
            freelist_page: 3,
            tx_id: 8,
            hash: 64,
        };

        assert!(!meta.valid());
        meta.hash = meta.hash_self();
        assert_eq!(meta.hash, meta.hash_self());

        meta.tx_id = 88;
        assert_ne!(meta.hash, meta.hash_self());

        meta.hash = meta.hash_self();
        assert_eq!(meta.hash, meta.hash_self());
    }
}
