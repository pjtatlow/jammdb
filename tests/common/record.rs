use bytes::{BufMut, Bytes, BytesMut};
use jammdb::{Bucket, Error, OpenOptions, Tx};
use rand::{distributions::Alphanumeric, prelude::*};
use std::{
    collections::BTreeMap,
    fs::File,
    io::{BufRead, BufReader, Write},
    path::Path,
};

pub struct SizeParams {
    pub min: usize,
    pub max: usize,
}

impl SizeParams {
    fn num(&self) -> usize {
        thread_rng().gen_range(self.min..=self.max)
    }
}

pub struct TestDetails {
    pub name: &'static str,
    pub page_size: usize,
    pub iterations: usize,
    pub inserts: SizeParams,
    pub updates: SizeParams,
    pub deletes: SizeParams,
    pub num_buckets: SizeParams,
    pub key_size: SizeParams,
    pub value_size: SizeParams,
    pub buckets: Vec<Vec<&'static str>>,
}

impl TestDetails {
    pub fn run(self) -> Result<(), Error> {
        let random_file = super::RandomFile::new();
        let db = OpenOptions::new()
            .pagesize(self.page_size as u64)
            .strict_mode(true)
            .open(&random_file)?;

        let mut instructions = Instructions::new(self.name)?;

        let mut data: FakeNode = FakeNode::Bucket(BTreeMap::new());
        for _iter in 0..self.iterations {
            {
                instructions.push(Instruction::StartTx);
                let tx = db.tx(true)?;

                let bucket_paths = self
                    .buckets
                    .choose_multiple(&mut thread_rng(), self.num_buckets.num());

                // Navigate to this bucket
                for path in bucket_paths {
                    instructions.push(Instruction::ResetBucket);

                    let bytes = Bytes::from_static(path[0].as_bytes());
                    instructions.push(Instruction::SubBucket(bytes.clone()));
                    let mut b: Bucket = tx.get_or_create_bucket(path[0])?;
                    let mut data = data.sub_bucket(bytes);

                    for name in &path[1..] {
                        let bytes = Bytes::from(name.as_bytes());
                        instructions.push(Instruction::SubBucket(bytes.clone()));
                        b = b.get_or_create_bucket(*name)?;
                        data = data.sub_bucket(bytes);
                    }
                    let data = data.unwrap_bucket();
                    let was_empty = data.is_empty();
                    let existing_keys: Vec<Bytes> = data
                        .iter()
                        .filter_map(|(k, v)| match v {
                            FakeNode::Bucket(_) => None,
                            FakeNode::Value(_) => Some(k.clone()),
                        })
                        .collect();

                    // Insert a random number of values
                    for _ in 0..self.inserts.num() {
                        let mut key = rand_bytes(self.key_size.num());
                        while data.contains_key(&key) {
                            key = rand_bytes(self.key_size.num());
                        }
                        let value = rand_bytes(self.value_size.num());
                        instructions.push(Instruction::InsertKV(key.clone(), value.clone()));
                        b.put(key.clone(), value.clone())?;
                        data.insert(key, FakeNode::Value(value));
                    }
                    if !was_empty {
                        // Update a random number of values
                        let update_keys =
                            existing_keys.choose_multiple(&mut thread_rng(), self.updates.num());
                        for key in update_keys {
                            let value = rand_bytes(self.value_size.num());

                            instructions.push(Instruction::InsertKV(key.clone(), value.clone()));
                            let db_existing = b.put(key, value.clone())?;
                            let existing = data.insert(key.clone(), FakeNode::Value(value.clone()));

                            assert!(db_existing.is_some());
                            assert!(existing.is_some());
                            let db_existing = db_existing.unwrap();
                            let existing = existing.unwrap();

                            let (k, v) = db_existing.kv();
                            assert_eq!(k, key.as_ref());
                            assert!(existing.is_value());
                            let value = existing.unwrap_value();

                            assert_eq!(v, value.as_ref());
                        }

                        // Delete a random number of values
                        let delete_keys =
                            existing_keys.choose_multiple(&mut thread_rng(), self.deletes.num());
                        for key in delete_keys {
                            instructions.push(Instruction::DeleteKey(key.clone()));
                            b.delete(key)?;
                            data.remove(key);
                        }
                    }
                }
                instructions.push(Instruction::EndTx);
                tx.commit()?;
            }
            db.check()?;
            {
                // Check the database to make sure everything is valid
                let tx = db.tx(false)?;
                for (bucket_data, bucket) in tx.buckets() {
                    let data = data.sub_bucket(Bytes::copy_from_slice(bucket_data.name()));
                    assert!(data.is_bucket());
                    let data_bucket = data.unwrap_bucket();
                    check_bucket(bucket, data_bucket)?;
                }
            }
        }
        instructions.delete();

        Ok(())
    }
}

fn check_bucket(
    db_bucket: Bucket,
    data_bucket: &mut BTreeMap<Bytes, FakeNode>,
) -> Result<(), Error> {
    let mut db_iter = db_bucket.cursor();
    let mut data_iter = data_bucket.iter_mut();

    loop {
        let db_data = db_iter.next();
        let fake_data = data_iter.next();
        match db_data {
            Some(db_data) => {
                assert!(fake_data.is_some());
                let (key, fake_data) = fake_data.unwrap();

                match db_data {
                    jammdb::Data::Bucket(b) => {
                        assert_eq!(b.name(), key.as_ref());
                        assert!(fake_data.is_bucket());
                        let data_bucket = fake_data.unwrap_bucket();
                        let db_bucket = db_bucket.get_bucket(b)?;
                        check_bucket(db_bucket, data_bucket)?;
                    }
                    jammdb::Data::KeyValue(kv) => {
                        let (k, v) = kv.kv();
                        let key = key.as_ref();

                        assert!(fake_data.is_value());

                        let fake_data = fake_data.unwrap_value();
                        let value = fake_data.as_ref();

                        assert_eq!(k, key);
                        assert_eq!(v, value);
                    }
                }
            }
            None => {
                assert!(fake_data.is_none());
                break;
            }
        }
    }
    Ok(())
}

enum FakeNode {
    Bucket(BTreeMap<Bytes, FakeNode>),
    Value(Bytes),
}

impl FakeNode {
    fn is_value(&self) -> bool {
        match self {
            Self::Bucket(_) => false,
            Self::Value(_) => true,
        }
    }
    fn is_bucket(&self) -> bool {
        match self {
            Self::Bucket(_) => true,
            Self::Value(_) => false,
        }
    }
    fn unwrap_bucket(&mut self) -> &mut BTreeMap<Bytes, FakeNode> {
        match self {
            Self::Bucket(b) => b,
            _ => unreachable!(),
        }
    }
    fn unwrap_value(&self) -> Bytes {
        match self {
            Self::Value(b) => b.clone(),
            Self::Bucket(_) => unreachable!(),
        }
    }

    fn sub_bucket(&mut self, name: Bytes) -> &mut FakeNode {
        match self {
            Self::Bucket(b) => {
                if b.get(&name).is_none() {
                    b.insert(name.clone(), FakeNode::Bucket(BTreeMap::new()));
                }
                return b.get_mut(&name).unwrap();
            }
            Self::Value(_) => unreachable!(),
        }
    }
}

fn rand_bytes(size: usize) -> Bytes {
    let buf = BytesMut::new();
    let mut w = buf.writer();
    for byte in rand::thread_rng().sample_iter(&Alphanumeric).take(size) {
        let _ = w.write(&[byte]);
    }

    w.into_inner().freeze()
}

use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
enum Instruction {
    StartTx,
    ResetBucket,
    InsertKV(Bytes, Bytes),
    SubBucket(Bytes),
    DeleteKey(Bytes),
    UpdateKV(Bytes, Bytes),
    EndTx,
}

struct Instructions {
    i: Vec<Instruction>,
    f: Option<std::fs::File>,
    path: Option<String>,
    delete: bool,
}

impl Instructions {
    pub fn new(name: &str) -> Result<Instructions, std::io::Error> {
        let record = match std::env::var("RECORD") {
            Ok(v) => v.as_str() == "true",
            Err(_) => false,
        };

        let mut filename = None;
        let f = if record {
            while filename.is_none() {
                let suffix: String = std::str::from_utf8(
                    rand::thread_rng()
                        .sample_iter(&Alphanumeric)
                        .take(8)
                        .collect::<Vec<u8>>()
                        .as_slice(),
                )
                .unwrap()
                .into();
                let proposed = format!("{}_{}.log", name, suffix);

                if !Path::new(&proposed).exists() {
                    filename = Some(proposed);
                }
            }
            let path = filename.as_ref().unwrap();
            println!("Recoding instructions to {}", path);
            Some(std::fs::File::create(path)?)
        } else {
            None
        };

        Ok(Instructions {
            i: vec![],
            f,
            path: filename,
            delete: false,
        })
    }

    pub fn push(&mut self, i: Instruction) {
        let b = serde_json::to_string(&i).unwrap();
        self.i.push(i);
        if let Some(f) = self.f.as_mut() {
            writeln!(f, "{}", b).unwrap();
        }
    }

    pub fn delete(mut self) {
        self.delete = true;
    }
}

impl Drop for Instructions {
    fn drop(&mut self) {
        if self.delete && self.path.is_some() {
            self.f = None;
            let _ = std::fs::remove_file(self.path.as_ref().unwrap());
        }
    }
}

pub fn log_playback(name: &str) -> Result<(), Error> {
    let log = BufReader::new(File::open(name)?);
    let mut instructions: Vec<Instruction> = Vec::new();
    for line in log.lines() {
        let line = line?;
        instructions.push(serde_json::from_str(line.as_str()).unwrap());
    }

    let random_file = super::RandomFile::new();

    let db = OpenOptions::new().pagesize(1024).open(&random_file)?;
    let mut root: FakeNode = FakeNode::Bucket(BTreeMap::new());
    // let mut data_b: &mut FakeNode = &mut data;

    let mut tx = None;
    let mut bucket_path: Vec<Bytes> = Vec::new();
    // let mut b: Option<Bucket> = None;
    let mut count = 0_u64;
    for instruction in instructions.iter() {
        count += 1;
        match instruction {
            Instruction::ResetBucket => {
                bucket_path.clear();
            }
            Instruction::StartTx => tx = Some(db.tx(true)?),
            Instruction::EndTx => {
                bucket_path.clear();
                let tx = tx.take().unwrap();

                tx.commit()?;
                db.check()?;

                {
                    // Check the database to make sure everything is valid
                    let tx = db.tx(false)?;

                    for (bucket_data, bucket) in tx.buckets() {
                        let data = root.sub_bucket(Bytes::copy_from_slice(bucket_data.name()));
                        assert!(data.is_bucket());
                        let data_bucket = data.unwrap_bucket();
                        check_bucket(bucket, data_bucket)?;
                    }
                }
            }
            Instruction::SubBucket(name) => {
                bucket_path.push(name.clone());
            }
            Instruction::InsertKV(k, v) => {
                mutate_buckets(
                    tx.as_ref().unwrap(),
                    &mut root,
                    &bucket_path,
                    |bucket, data_bucket| {
                        bucket.put(k, v)?;
                        data_bucket.insert(k.clone(), FakeNode::Value(v.clone()));
                        Ok(())
                    },
                )?;
            }
            Instruction::UpdateKV(k, v) => {
                mutate_buckets(
                    tx.as_ref().unwrap(),
                    &mut root,
                    &bucket_path,
                    |bucket, data_bucket| {
                        let existing = bucket.put(k, v)?;
                        assert!(existing.is_some());
                        data_bucket.insert(k.clone(), FakeNode::Value(v.clone()));
                        Ok(())
                    },
                )?;
            }
            Instruction::DeleteKey(k) => {
                mutate_buckets(
                    tx.as_ref().unwrap(),
                    &mut root,
                    &bucket_path,
                    |bucket, data_bucket| {
                        bucket.delete(k)?;
                        data_bucket.remove(k);
                        Ok(())
                    },
                )?;
            }
        }
    }
    let _ = count;
    Ok(())
}

fn mutate_buckets<'tx, F>(
    tx: &Tx<'tx>,
    root: &mut FakeNode,
    path: &Vec<Bytes>,
    f: F,
) -> Result<(), Error>
where
    F: Fn(&Bucket, &mut BTreeMap<Bytes, FakeNode>) -> Result<(), Error>,
{
    assert!(!path.is_empty());
    let mut b = tx.get_or_create_bucket(&path[0])?;
    let mut node = root.sub_bucket(path[0].clone());
    for name in path[1..].iter() {
        b = b.get_or_create_bucket(name)?;
        node = node.sub_bucket(name.clone());
    }
    let data_bucket = node.unwrap_bucket();
    f(&b, data_bucket)?;
    Ok(())
}
