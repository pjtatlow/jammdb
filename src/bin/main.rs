use jammdb::{DB, Error};

fn main() -> Result<(), Error> {
    std::fs::remove_file("test.db")?;
    {
        let db = DB::open("test.db")?;
        let mut tx = db.tx(true)?;
        let b = tx.create_bucket("abc")?;
        for i in 0..=1_u32 {
            b.put(i.to_be_bytes(), i.to_string())?;
        }
        tx.commit()?;
        println!("\n\n");
        // tx.print_graph();
    }
    println!("DONE WITH INITIAL WRITE");
    {
        let db = DB::open("test.db")?;
        let mut tx = db.tx(true)?;
        let b = tx.get_bucket("abc")?;
        
        // println!("A: {:#?}", b.get(1_u32.to_be_bytes()));
        b.put(1_u32.to_be_bytes(), "*11111111111*")?;
        // println!("B: {:#?}", b.get(1_u32.to_be_bytes()));
        tx.commit()?;
        println!("\n\n");
    }
    {
        let db = DB::open("test.db")?;
        let mut tx = db.tx(true)?;
        let b = tx.get_bucket("abc")?;
        
        // println!("A: {:#?}", b.get(1_u32.to_be_bytes()));
        b.put(1_u32.to_be_bytes(), "******1******")?;
        // println!("B: {:#?}", b.get(1_u32.to_be_bytes()));
        tx.commit()?;
        println!("\n\n");
    }
    {
        let db = DB::open("test.db")?;
        let mut tx = db.tx(true)?;
        let b = tx.get_bucket("abc")?;
        
        // println!("A: {:#?}", b.get(1_u32.to_be_bytes()));
        b.put(1_u32.to_be_bytes(), "*11111111111*")?;
        // println!("B: {:#?}", b.get(1_u32.to_be_bytes()));
        tx.commit()?;
        println!("\n\n");
    }
    {
        let db = DB::open("test.db")?;
        let mut tx = db.tx(true)?;
        let b = tx.get_bucket("abc")?;
        
        // println!("A: {:#?}", b.get(1_u32.to_be_bytes()));
        b.put(1_u32.to_be_bytes(), "******1******")?;
        // println!("B: {:#?}", b.get(1_u32.to_be_bytes()));
        tx.commit()?;
        println!("\n\n");
    }
    {
        let db = DB::open("test.db")?;
        let mut tx = db.tx(true)?;
        let b = tx.get_bucket("abc")?;
        
        // println!("A: {:#?}", b.get(1_u32.to_be_bytes()));
        b.put(1_u32.to_be_bytes(), "*11111111111*")?;
        // println!("B: {:#?}", b.get(1_u32.to_be_bytes()));
        tx.commit()?;
        println!("\n\n");
    }
    {
        let db = DB::open("test.db")?;
        let mut tx = db.tx(true)?;
        let b = tx.get_bucket("abc")?;
        
        // println!("A: {:#?}", b.get(1_u32.to_be_bytes()));
        b.put(1_u32.to_be_bytes(), "******1******")?;
        // println!("B: {:#?}", b.get(1_u32.to_be_bytes()));
        tx.commit()?;
        println!("\n\n");
    }
    {
        let db = DB::open("test.db")?;
        let mut tx = db.tx(true)?;
        let b = tx.get_bucket("abc")?;
        
        // println!("A: {:#?}", b.get(1_u32.to_be_bytes()));
        b.put(1_u32.to_be_bytes(), "*11111111111*")?;
        // println!("B: {:#?}", b.get(1_u32.to_be_bytes()));
        tx.commit()?;
        println!("\n\n");
    }
    {
        let db = DB::open("test.db")?;
        let mut tx = db.tx(true)?;
        let b = tx.get_bucket("abc")?;
        
        // println!("A: {:#?}", b.get(1_u32.to_be_bytes()));
        b.put(1_u32.to_be_bytes(), "******1******")?;
        // println!("B: {:#?}", b.get(1_u32.to_be_bytes()));
        tx.commit()?;
        println!("\n\n");
    }
    // {
    //     let db = DB::open("test.db")?;
    //     let mut tx = db.tx()?;
    //     let b = tx.get_bucket("abc")?;
            
    //     println!("C: {:#?}", b.get(8888_u32.to_be_bytes()));
    //     b.put(8888_u32.to_be_bytes(), "**8888**");
    //     println!("D: {:#?}", b.get(8888_u32.to_be_bytes()));
    //     tx.commit()?;
    println!("\n\n");
    // }
    
    // {
    //     let db = DB::open("test.db")?;
    //     let mut tx = db.tx()?;
    //     let b = tx.get_bucket("abc")?;
            
    //     println!("E: {:#?}", b.get(8888_u32.to_be_bytes()));
    //     b.put(8888_u32.to_be_bytes(), "8888");
    //     println!("F: {:#?}", b.get(8888_u32.to_be_bytes()));
    //     tx.commit()?;
    println!("\n\n");
    // }
    
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
