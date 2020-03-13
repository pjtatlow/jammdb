use jammdb::{DB, Error, Data, Bucket};

mod common;

#[test]
fn sibling_buckets() -> Result<(), Error> {
	let random_file = common::RandomFile::new();
	{
		let mut db = DB::open(&random_file.path)?;
		{
			let mut tx = db.tx(true)?;
			let b = tx.create_bucket("abc")?;
			for i in 0..=10_u64 {
				b.put(i.to_be_bytes(), i.to_string())?;
			}
			check_data(&b, 11, 1, vec![]);
			tx.commit()?;
		}
		{
			let mut tx = db.tx(true)?;

			let b = tx.get_bucket("abc")?;
			check_data(&b, 11, 1, vec![]);
			for i in 0..=10_u64 {
				b.put(i.to_be_bytes(), i.to_string().repeat(4))?;
			}
			check_data(&b, 11, 4, vec![]);
			
			let b2 = tx.create_bucket("def")?;
			for i in 0..=900_u64 {
				b2.put(i.to_be_bytes(), i.to_string().repeat(2))?;
			}
			check_data(&b2, 901, 2, vec![]);
		
			tx.commit()?;
		}
		{
			let mut tx = db.tx(true)?;
			let b = tx.get_bucket("abc")?;
			check_data(&b, 11, 4, vec![]);

			let b2 = tx.get_bucket("def")?;
			check_data(&b2, 901, 2, vec![]);
		}
	}
	{
		let mut db = DB::open(&random_file.path)?;
		let mut tx = db.tx(true)?;
		{
			let b = tx.get_bucket("abc")?;
			check_data(&b, 11, 4, vec![]);
		}
		{
			let b2 = tx.get_bucket("def")?;
			check_data(&b2, 901, 2, vec![]);
		}
	}
	Ok(())
}

#[test]
fn nested_buckets() -> Result<(), Error> {
	let random_file = common::RandomFile::new();
	{
		let mut db = DB::open(&random_file.path)?;
		{
			let mut tx = db.tx(true)?;
			let b = tx.create_bucket("abc")?;
			for i in 0..=10_u64 {
				b.put(i.to_be_bytes(), i.to_string().repeat(2))?;
			}
			check_data(&b, 11, 2, vec![]);
			let b = b.create_bucket("def")?;
			for i in 0..=10_u64 {
				b.put(i.to_be_bytes(), i.to_string().repeat(4))?;
			}
			check_data(&b, 11, 4, vec![]);
			let b = b.create_bucket("ghi")?;
			for i in 0..=10_u64 {
				b.put(i.to_be_bytes(), i.to_string().repeat(8))?;
			}
			check_data(&b, 11, 8, vec![]);
			tx.commit()?;
		}
		{
			let mut tx = db.tx(true)?;

			let b = tx.get_bucket("abc")?;
			check_data(&b, 12, 2, vec![Vec::from("def".as_bytes())]);

			let b = b.get_bucket("def")?;
			check_data(&b, 12, 4, vec![Vec::from("ghi".as_bytes())]);

			let b = b.get_bucket("ghi")?;
			check_data(&b, 11, 8, vec![]);

			tx.commit()?;
		}
	}
	Ok(())
}

fn check_data(b: &Bucket, len: u64, repeats: usize, bucket_names: Vec<Vec<u8>>) {
	let mut count: u64 = 0;
	for (i, data) in b.cursor().into_iter().enumerate() {
		let i = i as u64;
		count += 1;
		match data {
			Data::KeyValue(kv) => {
				assert_eq!(kv.key(), i.to_be_bytes());
				assert_eq!(kv.value(), i.to_string().repeat(repeats).as_bytes());
			},
			Data::Bucket(b) => {
				assert!(bucket_names.contains(&Vec::from(b.name())));
			},
		};		
	}
	assert_eq!(count, len);
}
