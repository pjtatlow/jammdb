use jammdb::{Bucket, Data, Error, DB};

mod common;

#[test]
fn tx_isolation() -> Result<(), Error> {
	let random_file = common::RandomFile::new();
	let mut db = DB::open(&random_file.path)?;
	let mut db2 = db.clone();
	{
		let mut ro_tx = db.tx(false)?;
		let mut wr_tx = db2.tx(true)?;
		let b = wr_tx.create_bucket("abc123")?;

		for i in 0..=10_u64 {
			assert_eq!(b.next_int(), i);
			b.put(i.to_be_bytes(), i.to_string())?;
		}
		assert_eq!(b.next_int(), 11);

		if let Err(e) = ro_tx.get_bucket("abc123") {
			match e {
				Error::BucketMissing => (),
				_ => panic!("Unexpected error {:?}", e),
			}
		} else {
			panic!("Expected err");
		}
		wr_tx.commit()?;
	}
	{
		let mut ro_tx = db.tx(false)?;
		let ro_b = ro_tx.get_bucket("abc123")?;
		check_data(ro_b, 11, 1);
		let mut rw_tx = db2.tx(true)?;
		let rw_b = rw_tx.get_bucket("abc123")?;
		check_data(rw_b, 11, 1);
		assert_eq!(ro_b.next_int(), 11);
		assert_eq!(rw_b.next_int(), 11);
		for i in 0..=100_u64 {
			if i < 11 {
				assert_eq!(rw_b.next_int(), 11);
			} else {
				assert_eq!(rw_b.next_int(), i);
			}
			assert_eq!(ro_b.next_int(), 11);
			rw_b.put(i.to_be_bytes(), i.to_string().repeat(4))?;
		}
		assert_eq!(rw_b.next_int(), 101);
		check_data(rw_b, 101, 4);
		check_data(ro_b, 11, 1);
	}

	Ok(())
}

fn check_data(b: &Bucket, len: u64, repeats: usize) {
	let mut count: u64 = 0;
	for (i, data) in b.cursor().into_iter().enumerate() {
		let i = i as u64;
		count += 1;
		match data {
			Data::KeyValue(kv) => {
				assert_eq!(kv.key(), i.to_be_bytes());
				assert_eq!(kv.value(), i.to_string().repeat(repeats).as_bytes());
			}
			_ => panic!("Expected Data::KeyValue"),
		};
	}
	assert_eq!(count, len);
}
