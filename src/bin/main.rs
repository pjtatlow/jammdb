use jammdb::{DB, Transaction, Error, Data};

fn main() -> Result<(), Error> {
    std::fs::remove_file("test.db");
    {
        let db = DB::open("test.db")?;
        let mut tx = db.tx()?;
        {
            let b = tx.create_bucket("abc")?;
            for i in 0..500 {
                b.put(i.to_string(), i.to_string());
            }
            // b.put("FOO", "BAR");
            // b.put("BAR", "BAZ");
            // b.put("BAZ", "FOO");
        }
        // {
        //     let b = tx.create_bucket("def")?;
        //     b.put("123", "456");
        //     b.put("789", "012");
        //     b.put("345", "678");
        //     let b2 = b.create_bucket("456")?;
        //     b2.put("678", "abc");
        //     b2.put("901", "def");
        //     b2.put("234", "ghi");
        // }
        // if let Some(data) = b.get("FOO") {
        //     if let Data::KeyValue(kv) = data {
        //         println!("KEY: {}", unsafe{std::str::from_utf8_unchecked(kv.key())});
        //         println!("VALUE: {}", unsafe{std::str::from_utf8_unchecked(kv.value())});
        //     } else {
        //         println!("UHHHH");
        
        //     }
        // } else {
        //     println!("NO DATA");
        // }
        tx.commit()?;
        tx.print_graph();
    }
    
    {
        // let db = DB::open("test.db")?;
        // let mut tx = db.tx()?;
        // for key in vec!["abc", "def"] {
        //     let b = tx.get_bucket(key)?;
        //     for data in b.cursor() {
        //         match data {
        //             Data::KeyValue(kv) => {
        //                 println!("KEY: {}, VALUE: {}", unsafe{std::str::from_utf8_unchecked(kv.key())}, unsafe{std::str::from_utf8_unchecked(kv.value())});
        //             },
        //             Data::Bucket(b) => {
        //                 println!("Bucket Name: {}", unsafe{std::str::from_utf8_unchecked(b.name())});
        //             // println!("Bucket Meta: {:?}", b.meta());
        //             }
        //         }
        //     }
        // }
        // for key in vec!["FOO", "BAR", "BAZ"] {
        //     if let Some(data) = b.get(key) {
        //         if let Data::KeyValue(kv) = data {
        //         } else {
        //             println!("UHHHH");
                    
        //         }
        //     } else {
        //         println!("NO DATA");
        //     }
        // }
    }
    
    // let mut v: Vec<std::thread::JoinHandle<_>> = vec![];
    // for i in 0..10 {
    //     let db = db.clone();x
    //     let j = std::thread::spawn(move || {
    //         let tx = db.tx();
    //     });
    //     v.push(j);
    // }

    // for j in v {
    //     j.join().unwrap();
    // }


    Ok(())
}


// use std::io::Write;
// use memmap::Mmap;

// const SIZE: usize = std::mem::size_of::<T>();

// fn main() {
//     write_to_file().unwrap();
//     read_from_file().unwrap();
// }

// struct T  {
//     a: usize,
//     b: usize,
// }

// fn write_to_file() -> std::io::Result<()> {
//     let mut file = std::fs::OpenOptions::new()
//         .create(true)
//         .write(true)
//         .truncate(true)
//         .open("test.db")
//         .unwrap();

//     let t = T{a: 456, b: 789};
//     let ptr = &t as *const T as *const [u8; SIZE];    
//     let arr: &[u8; SIZE] = unsafe{ ptr.as_ref().unwrap() };
//     file.write("🤷🏽‍♂️".as_bytes())?;
//     file.write(&arr[..])?;
//     Ok(())
// }

// fn read_from_file() -> std::io::Result<()> {
//     let file = std::fs::File::open("test.db")?;
//     let mmap = unsafe { Mmap::map(&file)? };
//     let ptr = mmap["🤷🏽‍♂️".as_bytes().len()..].as_ptr() as *const T;
//     let t: &T = unsafe{ ptr.as_ref().unwrap() };
//     println!("a: {}, b: {}", t.a, t.b);
//     Ok(())
// }