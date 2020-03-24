use jammdb::{Data, Error, DB};
use std::collections::HashSet;

mod common;

#[test]
fn delete_random() -> Result<(), Error> {
	let random_file = common::RandomFile::new();
	let mut deleted = HashSet::new();
	let mut rng = rand::thread_rng();
	{
		let mut db = DB::open(&random_file.path)?;
		{
			let mut tx = db.tx(true)?;
			let b = tx.create_bucket("abc")?;
			for i in 0..1000_u64 {
				b.put(i.to_be_bytes(), i.to_string())?;
			}
			tx.commit()?;
		}
		{
			let mut tx = db.tx(true)?;
			let b = tx.get_bucket("abc")?;
			for i in rand::seq::index::sample(&mut rng, 1000, 100).into_iter() {
				let i = i as u64;
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
			for i in 0..1000_u64 {
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
			for i in 0..1000_u64 {
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
	}
	Ok(())
}
