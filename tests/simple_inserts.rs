use jammdb::{Bucket, Data, Error, OpenOptions, DB};
use rand::prelude::*;

mod common;

#[test]
fn super_simple() -> Result<(), Error> {
    test_insert((0..=1).collect())?;
    Ok(())
}

#[test]
fn small_insert() -> Result<(), Error> {
    test_insert((0..=100).collect())?;
    test_insert((0..=100).collect())?;
    test_insert((0..=100).collect())?;
    Ok(())
}

#[test]
fn medium_insert() -> Result<(), Error> {
    test_insert((0..=1000).collect())?;
    test_insert((0..=1000).collect())?;
    test_insert((0..=1000).collect())?;
    Ok(())
}

#[test]
fn large_insert() -> Result<(), Error> {
    test_insert((0..=50000).collect())?;
    test_insert((0..=50000).collect())?;
    test_insert((0..=50000).collect())?;
    Ok(())
}

fn test_insert(mut values: Vec<u64>) -> Result<(), Error> {
    let random_file = common::RandomFile::new();
    let mut rng = rand::thread_rng();
    {
        let db = OpenOptions::new()
            .strict_mode(true)
            .open(&random_file.path)?;
        {
            let tx = db.tx(true)?;
            let b = tx.create_bucket("abc")?;
            // insert data in a random order
            values.shuffle(&mut rng);
            for i in values.iter() {
                let existing = b.put(i.to_be_bytes(), i.to_string())?;
                assert!(existing.is_none());
            }
            // check before commit
            check_data(&b, values.len() as u64, 1);
            assert_eq!(b.next_int(), values.len() as u64);
            tx.commit()?;
        }
        {
            let tx = db.tx(false)?;
            let b = tx.get_bucket("abc")?;
            // check after commit before closing file
            check_data(&b, values.len() as u64, 1);
            assert_eq!(b.next_int(), values.len() as u64);
        }
    }
    {
        let db = DB::open(&random_file.path)?;
        let tx = db.tx(false)?;
        let b = tx.get_bucket("abc")?;
        // check after re-opening file
        check_data(&b, values.len() as u64, 1);
        assert_eq!(b.next_int(), values.len() as u64);
        let missing_key = (values.len() + 1) as u64;
        assert!(b.get(missing_key.to_be_bytes()).is_none());
    }
    let db = DB::open(&random_file.path)?;
    db.check()
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
