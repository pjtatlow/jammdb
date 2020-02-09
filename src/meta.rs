use crate::bucket::{BucketMeta};
use crate::page::PageID;

#[repr(C)]
#[derive(Debug, Clone)]
pub (crate) struct Meta {
	pub (crate) magic: u32,
	pub (crate) version: u32,
	pub (crate) pagesize: u32,
	pub (crate) flags: u32,
	pub (crate) root: BucketMeta,
	pub (crate) num_pages: PageID,
	pub (crate) freelist_page: PageID,
}