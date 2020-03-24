use jammdb::{Error, DB};

fn main() -> Result<(), Error> {
    if std::path::PathBuf::from("test.db").is_file() {
        std::fs::remove_file("test.db")?;
    }
    let mut db = DB::open("test.db")?;
    {
        let mut tx = db.tx(true)?;
        let b = tx.create_bucket("abc")?;
        for i in 0..=150_u64 {
            b.put(i.to_be_bytes(), i.to_string())?;
        }
        tx.commit()?;
    }
    {
        let mut tx = db.tx(true)?;
        let b = tx.get_bucket("abc")?;
        println!("A {:?}", b.delete(88_u64.to_be_bytes())?);
        println!("B {:?}", b.delete(140_u64.to_be_bytes())?);
        println!("C {:?}", b.delete(0_u64.to_be_bytes())?);
        println!("C {:?}", b.delete(48_u64.to_be_bytes())?);
        println!("C {:?}", b.delete(95_u64.to_be_bytes())?);
        // println!("B: {:#?}", b.get(1_u32.to_be_bytes()));
        tx.commit()?;
    }
    {
        let mut tx = db.tx(true)?;
        let b = tx.get_bucket("abc")?;
        b.put(0_u64.to_be_bytes(), "00000000000000000")?;
        // println!("A {:?}", b.delete(88_u64.to_be_bytes())?);
        // println!("B {:?}", b.delete(140_u64.to_be_bytes())?);
        // println!("C {:?}", b.delete(0_u64.to_be_bytes())?);
        // println!("C {:?}", b.delete(48_u64.to_be_bytes())?);
        // println!("C {:?}", b.delete(95_u64.to_be_bytes())?);
        // println!("B: {:#?}", b.get(1_u32.to_be_bytes()));
        tx.commit()?;
    }
    {
        let mut tx = db.tx(true)?;
        let b = tx.get_bucket("abc")?;
        for i in 0..=150_u64 {
            println!("{:?}", b.get(i.to_be_bytes()));
        }
        tx.print_graph();
    }

    Ok(())
}
