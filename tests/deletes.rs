use jammdb::{Data, Error, DB};
use rand::prelude::*;
use std::collections::HashSet;

mod common;

#[test]
fn small_deletes() {
	for _ in 0..10 {
		test_deletes(100).unwrap();
	}
}

#[test]
fn medium_deletes() {
	for _ in 0..10 {
		test_deletes(1000).unwrap();
	}
}

#[test]
fn large_deletes() {
	for _ in 0..10 {
		test_deletes(1000).unwrap();
	}
}

fn test_deletes(highest_int: u64) -> Result<(), Error> {
	let random_file = common::RandomFile::new();
	let mut deleted = HashSet::new();
	let mut rng = rand::thread_rng();
	{
		let mut db = DB::open(&random_file.path)?;
		{
			let mut tx = db.tx(true)?;
			let b = tx.create_bucket("abc")?;
			for i in 0..highest_int {
				b.put(i.to_be_bytes(), i.to_string())?;
			}
			tx.commit()?;
		}
		let mut ids_to_delete: Vec<u64> = (0..highest_int).collect();
		ids_to_delete.shuffle(&mut rng);
		let mut id_iter = ids_to_delete.iter();

		loop {
			{
				let mut tx = db.tx(true)?;
				let b = tx.get_bucket("abc")?;
				// delete between 0 and 100 random items
				for _ in 0..rng.gen_range(10, 100) {
					let i = id_iter.next();
					if i.is_none() {
						break;
					}
					let i = i.unwrap();
					if deleted.insert(i) {
						match b.delete(i.to_be_bytes())? {
							Data::KeyValue(kv) => {
								assert_eq!(kv.key(), i.to_be_bytes());
								assert_eq!(kv.value(), i.to_string().as_bytes());
							}
							Data::Bucket(_) => panic!("Expected Data::KeyValue"),
						}
					}
				}
				for i in 0..highest_int {
					let data = b.get(i.to_be_bytes());
					if deleted.contains(&i) {
						assert_eq!(data, None)
					} else {
						match data {
							Some(Data::KeyValue(kv)) => {
								assert_eq!(kv.key(), i.to_be_bytes());
								assert_eq!(kv.value(), i.to_string().as_bytes());
							}
							_ => panic!("Expected Data::KeyValue"),
						}
					}
				}
				tx.commit()?;
			}
			{
				let mut tx = db.tx(true)?;
				let b = tx.get_bucket("abc")?;
				for i in 0..highest_int {
					let data = b.get(i.to_be_bytes());
					if deleted.contains(&i) {
						assert_eq!(data, None)
					} else {
						match data {
							Some(Data::KeyValue(kv)) => {
								assert_eq!(kv.key(), i.to_be_bytes());
								assert_eq!(kv.value(), i.to_string().as_bytes());
							}
							_ => panic!("Expected Some(Data::KeyValue) at index {}: {:?}", i, data),
						}
					}
				}
			}
			if deleted.len() == ids_to_delete.len() {
				break;
			}
		}
		db.check()
	}
}
