use jammdb::{Bucket, Data, Error, OpenOptions, DB};
use rand::prelude::*;

mod common;

#[test]
fn cursor_seek() -> Result<(), Error> {
    // list from https://simple.wikipedia.org/wiki/List_of_fruits
    let mut fruits = vec![
        "açaí",
        "ackee",
        "apple",
        "apricot",
        "avocado",
        "banana",
        "bilberry",
        "black sapote",
        "blackberry",
        "blackcurrant",
        "blood orange",
        "blueberry",
        "boysenberry",
        "breadfruit",
        "cactus pear",
        "cantaloupe",
        "cherimoya",
        "cherry",
        "chico fruit",
        "clementine",
        "cloudberry",
        "coconut",
        "crab apple",
        "cranberry",
        "currant",
        "damson",
        "date",
        "dragonfruit",
        "durian",
        "elderberry",
        "feijoa",
        "fig",
        "galia melon",
        "goji berry",
        "gooseberry",
        "grape",
        "grapefruit",
        "guava",
        "hala fruit",
        "honeyberry",
        "honeydew",
        "huckleberry",
        "jabuticaba",
        "jackfruit",
        "jambul",
        "japanese plum",
        "jostaberry",
        "jujube",
        "juniper berry",
        "kiwano",
        "kiwifruit",
        "kumquat",
        "lemon",
        "lime",
        "loganberry",
        "longan",
        "loquat",
        "lychee",
        "mandarine",
        "mango",
        "mangosteen",
        "marionberry",
        "melon",
        "miracle fruit",
        "monstera delisiousa",
        "mulberry",
        "nance",
        "nectarine",
        "orange",
        "papaya",
        "passionfruit",
        "peach",
        "pear",
        "persimmon",
        "pineapple",
        "pineberry",
        "plantain",
        "plum",
        "plumcot",
        "pomegranate",
        "pomelo",
        "prune",
        "purple mangosteen",
        "quince",
        "raisin",
        "rambutan",
        "raspberry",
        "redcurrant",
        "salak",
        "salal berry",
        "salmonberry",
        "satsuma",
        "soursop",
        "star apple",
        "star fruit",
        "strawberry",
        "surinam cherry",
        "tamarillo",
        "tamarind",
        "tangelo",
        "tangerine",
        "tayberry",
        "ugli fruit",
        "watermelon",
        "white currant",
        "white sapote",
        "yuzu",
    ];
    fruits.sort_unstable();

    let random_file = common::RandomFile::new();
    {
        let db = OpenOptions::new()
            .strict_mode(true)
            .open(&random_file.path)?;
        {
            let tx = db.tx(true)?;
            let b = tx.create_bucket("abc")?;
            {
                let mut random_fruits = Vec::from(fruits.as_slice());
                let mut rng = rand::thread_rng();
                random_fruits.shuffle(&mut rng);
                // randomly insert the fruits into the bucket
                for fruit in random_fruits.iter() {
                    let index = fruits.binary_search(fruit).unwrap();
                    b.put(*fruit, index.to_string())?;
                }
            }
            check_cursor_starts(&fruits, &b);
            tx.commit()?;
        }
        {
            let tx = db.tx(false)?;
            let b = tx.get_bucket("abc")?;
            check_cursor_starts(&fruits, &b);
        }
    }
    {
        let db = OpenOptions::new()
            .strict_mode(true)
            .open(&random_file.path)?;
        {
            let tx = db.tx(false)?;
            let b = tx.get_bucket("abc")?;
            check_cursor_starts(&fruits, &b);
        }
        {
            let tx = db.tx(false)?;
            let b = tx.get_bucket("abc")?;
            check_cursor("bl", &fruits[6..], &b, 6);
        }
        {
            let tx = db.tx(true)?;
            let b = tx.get_bucket("abc")?;
            b.put("zomato", fruits.len().to_string())?;
            fruits.push("zomato");
            check_cursor_starts(&fruits, &b);
            tx.commit()?;
        }
        {
            let tx = db.tx(false)?;
            let b = tx.get_bucket("abc")?;
            check_cursor("bl", &fruits[6..], &b, 6);
        }
    }
    let db = DB::open(&random_file.path)?;
    db.check()
}

// checks every start position and checks that you can iterate
// starting from there
fn check_cursor_starts(fruits: &Vec<&str>, b: &Bucket) {
    // randomly seek over the bucket
    let mut random_fruits = Vec::from(fruits.as_slice());
    let mut rng = rand::thread_rng();
    random_fruits.shuffle(&mut rng);
    for fruit in random_fruits.iter() {
        let start_index = fruits.binary_search(fruit).unwrap();
        let expected_fruits = &fruits[start_index..];
        check_cursor(fruit, expected_fruits, b, start_index);
    }
}

fn check_cursor(seek_to: &str, expected_fruits: &[&str], b: &Bucket, start_index: usize) {
    let mut cur_index = 0;
    let mut cursor = b.cursor();
    let exists = cursor.seek(seek_to);
    if expected_fruits[0] == seek_to {
        assert!(exists);
    }
    for data in cursor {
        assert!(cur_index < expected_fruits.len());
        let expected_fruit = expected_fruits[cur_index];
        if let Data::KeyValue(kv) = data {
            assert_eq!(expected_fruit.as_bytes(), kv.key());
            let value_string = (cur_index + start_index).to_string();
            assert_eq!(value_string.as_bytes(), kv.value());
        } else {
            panic!("Expected Data::KeyValue");
        }
        cur_index += 1;
    }
    assert_eq!(cur_index, expected_fruits.len());
}

#[test]
fn root_buckets() -> Result<(), Error> {
    let random_file = common::RandomFile::new();
    {
        let db = OpenOptions::new().strict_mode(true).open(&random_file)?;
        {
            let tx = db.tx(true)?;
            {
                let b = tx.create_bucket("abc")?;
                b.put("data", "one")?;
            }
            {
                let b = tx.create_bucket("def")?;
                b.put("data", "two")?;
            }
            {
                let b = tx.create_bucket("ghi")?;
                b.put("data", "three")?;
            }
            tx.commit()?;
        }
        let tx = db.tx(false)?;
        for (i, (data, bucket)) in tx.buckets().enumerate() {
            let name = std::str::from_utf8(data.name()).unwrap();
            let kv = bucket.get_kv("data").unwrap();
            let value = std::str::from_utf8(kv.value()).unwrap();
            if i == 0 {
                assert_eq!(name, "abc");
                assert_eq!(value, "one");
            } else if i == 1 {
                assert_eq!(name, "def");
                assert_eq!(value, "two");
            } else if i == 2 {
                assert_eq!(name, "ghi");
                assert_eq!(value, "three");
            } else {
                panic!("TOO MANY BUCKETS!")
            }
        }
    };
    Ok(())
}

#[test]
fn kv_iter() -> Result<(), Error> {
    let random_file = common::RandomFile::new();
    let data = vec![("abc", "one"), ("def", "two"), ("ghi", "three")];
    {
        let db = OpenOptions::new().strict_mode(true).open(&random_file)?;
        {
            let tx = db.tx(true)?;
            {
                let b = tx.create_bucket("data")?;
                for (k, v) in data.iter() {
                    b.put(*k, *v)?;
                }
            }
            tx.commit()?;
        }
        let tx = db.tx(false)?;
        let b = tx.get_bucket("data")?;
        for ((k, v), kvpair) in data.into_iter().zip(b.kv_pairs()) {
            assert_eq!(k.as_bytes(), kvpair.key());
            assert_eq!(v.as_bytes(), kvpair.value());
        }
    };
    Ok(())
}
