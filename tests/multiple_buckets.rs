use jammdb::{Bucket, Data, Error, DB};

mod common;

#[test]
fn sibling_buckets() -> Result<(), Error> {
    let random_file = common::RandomFile::new();
    {
        let db = DB::open(&random_file.path)?;
        {
            let tx = db.tx(true)?;
            let b = tx.create_bucket("abc")?;
            for i in 0..=10_u64 {
                let existing = b.put(i.to_be_bytes(), i.to_string())?;
                assert!(existing.is_none());
            }
            check_data(&b, 11, 1, vec![]);
            assert_eq!(b.next_int(), 11);
            tx.commit()?;
        }
        {
            let tx = db.tx(true)?;

            let b = tx.get_bucket("abc")?;
            check_data(&b, 11, 1, vec![]);
            for i in 0..=10_u64 {
                let existing = b.put(i.to_be_bytes(), i.to_string().repeat(4))?;
                assert!(existing.is_some());
                let kv = existing.unwrap();
                assert_eq!(kv.key(), i.to_be_bytes());
                assert_eq!(kv.value(), i.to_string().as_bytes());
            }
            check_data(&b, 11, 4, vec![]);
            assert_eq!(b.next_int(), 11);
            let b2 = tx.create_bucket("def")?;
            for i in 0..=900_u64 {
                b2.put(i.to_be_bytes(), i.to_string().repeat(2))?;
            }
            check_data(&b2, 901, 2, vec![]);
            assert_eq!(b2.next_int(), 901);
            tx.commit()?;
        }
        {
            let tx = db.tx(true)?;
            let b = tx.get_bucket("abc")?;
            check_data(&b, 11, 4, vec![]);
            assert_eq!(b.next_int(), 11);

            let b2 = tx.get_bucket("def")?;
            check_data(&b2, 901, 2, vec![]);
            assert_eq!(b2.next_int(), 901);
        }
    }
    {
        let db = DB::open(&random_file.path)?;
        let tx = db.tx(true)?;
        {
            let b = tx.get_bucket("abc")?;
            check_data(&b, 11, 4, vec![]);
        }
        {
            let b2 = tx.get_bucket("def")?;
            check_data(&b2, 901, 2, vec![]);
        }
    }
    let db = DB::open(&random_file.path)?;
    db.check()
}

#[test]
fn nested_buckets() -> Result<(), Error> {
    let random_file = common::RandomFile::new();
    {
        let db = DB::open(&random_file.path)?;
        {
            let tx = db.tx(true)?;
            let b = tx.create_bucket("abc")?;
            for i in 0..=10_u64 {
                let existing = b.put(i.to_be_bytes(), i.to_string().repeat(2))?;
                assert!(existing.is_none());
            }
            assert_eq!(b.next_int(), 11);
            check_data(&b, 11, 2, vec![]);
            let b = b.create_bucket("def")?;
            for i in 0..=100_u64 {
                let existing = b.put(i.to_be_bytes(), i.to_string().repeat(4))?;
                assert!(existing.is_none());
            }
            assert_eq!(b.next_int(), 101);
            check_data(&b, 101, 4, vec![]);
            let b = b.create_bucket("ghi")?;
            for i in 0..=1000_u64 {
                let existing = b.put(i.to_be_bytes(), i.to_string().repeat(8))?;
                assert!(existing.is_none());
            }
            assert_eq!(b.next_int(), 1001);
            check_data(&b, 1001, 8, vec![]);
            tx.commit()?;
        }
        {
            let tx = db.tx(true)?;

            let b = tx.get_bucket("abc")?;
            check_data(&b, 12, 2, vec![Vec::from("def".as_bytes())]);
            assert_eq!(b.next_int(), 12);

            let b = b.get_bucket("def")?;
            check_data(&b, 102, 4, vec![Vec::from("ghi".as_bytes())]);
            assert_eq!(b.next_int(), 102);

            let b = b.get_bucket("ghi")?;
            check_data(&b, 1001, 8, vec![]);
            assert_eq!(b.next_int(), 1001);

            tx.commit()?;
        }
    }
    let db = DB::open(&random_file.path)?;
    db.check()
}

fn check_data(b: &Bucket, len: u64, repeats: usize, bucket_names: Vec<Vec<u8>>) {
    let mut count: u64 = 0;
    for (i, data) in b.cursor().into_iter().enumerate() {
        let i = i as u64;
        count += 1;
        match &*data {
            Data::KeyValue(kv) => {
                assert_eq!(kv.key(), i.to_be_bytes());
                assert_eq!(kv.value(), i.to_string().repeat(repeats).as_bytes());
            }
            Data::Bucket(b) => {
                assert!(bucket_names.contains(&Vec::from(b.name())));
            }
        };
    }
    assert_eq!(count, len);
}

#[test]
fn empty_nested_buckets() -> Result<(), Error> {
    let random_file = common::RandomFile::new();
    {
        let db = DB::open(&random_file.path)?;
        {
            let tx = db.tx(true)?;
            let _root = tx.get_or_create_bucket("ROOT")?;
            tx.commit()?;
        }
        {
            let tx = db.tx(true)?;
            let root = tx.get_or_create_bucket("ROOT")?;
            let _child = root.get_or_create_bucket("CHILD")?;
            tx.commit()?;
        }
        {
            let tx = db.tx(true)?;
            let root = tx.get_or_create_bucket("ROOT")?;
            let child = root.get_or_create_bucket("CHILD")?;
            child.put("A", "B")?;
            // let _grandchild = child.get_or_create_bucket("GRANDCHILD")?;
            tx.commit()?;
        }
        {
            let tx = db.tx(true)?;
            let root = tx.get_or_create_bucket("ROOT")?;
            let child = root.get_or_create_bucket("CHILD")?;
            let _grandchild = child.get_or_create_bucket("GRANDCHILD")?;
            tx.commit()?;
        }
        {
            let tx = db.tx(true)?;
            let root = tx.get_or_create_bucket("ROOT")?;
            let child = root.get_or_create_bucket("CHILD")?;
            let grandchild = child.get_or_create_bucket("GRANDCHILD")?;
            let _greatgrandchild = grandchild.get_or_create_bucket("GREATGRANDCHILD")?;
            tx.commit()?;
        }
    }
    let db = DB::open(&random_file.path)?;
    db.check()
}
