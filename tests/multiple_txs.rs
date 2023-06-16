use jammdb::{Bucket, Data, Error, OpenOptions};

mod common;

#[test]
fn tx_isolation() -> Result<(), Error> {
    let random_file = common::RandomFile::new();
    let db = OpenOptions::new().strict_mode(true).open(&random_file)?;
    {
        let ro_tx = db.tx(false)?;
        let wr_tx = db.tx(true)?;
        let b = wr_tx.create_bucket("abc123")?;

        for i in 0..=10_u64 {
            assert_eq!(b.next_int(), i);
            assert!((b.put(i.to_be_bytes(), i.to_string())?).is_none());
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
        let ro_tx = db.tx(false)?;
        let ro_b = ro_tx.get_bucket("abc123")?;
        check_data(&ro_b, 11, 1);
        let rw_tx = db.tx(true)?;
        let rw_b = rw_tx.get_bucket("abc123")?;
        check_data(&rw_b, 11, 1);
        assert_eq!(ro_b.next_int(), 11);
        assert_eq!(rw_b.next_int(), 11);
        for i in 0..=100_u64 {
            let next_int = rw_b.next_int();
            let existing_data = rw_b.put(i.to_be_bytes(), i.to_string().repeat(4))?;
            if i < 11 {
                assert_eq!(next_int, 11);
                assert!(existing_data.is_some());
                let kv = existing_data.unwrap();
                assert_eq!(kv.key(), i.to_be_bytes());
                assert_eq!(kv.value(), i.to_string().as_bytes());
            } else {
                assert_eq!(next_int, i);
                assert!(existing_data.is_none());
            }
            assert_eq!(ro_b.next_int(), 11);
        }
        assert_eq!(rw_b.next_int(), 101);
        check_data(&rw_b, 101, 4);
        check_data(&ro_b, 11, 1);
    }
    db.check()
}

fn check_data(b: &Bucket, len: u64, repeats: usize) {
    let mut count: u64 = 0;
    for (i, data) in b.cursor().enumerate() {
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
