use jammdb::{DB, Error};

fn main() -> Result<(), Error> {
    std::fs::remove_file("test.db")?;
    {
        let mut db = DB::open("test.db")?;
        let mut tx = db.tx(true)?;
        let b = tx.create_bucket("abc")?;
        for i in 0..=1_u32 {
            b.put(i.to_be_bytes(), i.to_string())?;
        }
        
        let b2 = tx.create_bucket("DEF")?;
        for i in 0..=1_u32 {
            b2.put(i.to_be_bytes(), i.to_string())?;
        }

        tx.commit()?;
        println!("\n\n");
        // tx.print_graph();
    }
    println!("DONE WITH INITIAL WRITE");
    {
        let mut db = DB::open("test.db")?;
        let mut db2 = db.clone();
        let mut ro_tx = db.tx(false)?;
        // println!("RO: {:#?}", ro_b.get(1_u32.to_be_bytes()));
        {
            let mut tx = db2.tx(true)?;
            let b = tx.get_bucket("abc")?;
            
            println!("A: {:#?}", b.get(1_u32.to_be_bytes()));
            b.put(1_u32.to_be_bytes(), "*11111111111*")?;
            // println!("B: {:#?}", b.get(1_u32.to_be_bytes()));
            tx.commit()?;
        }
        let ro_b = ro_tx.get_bucket("abc")?;
        println!("RO: {:#?}", ro_b.get(1_u32.to_be_bytes()));
        {
            let mut tx = db2.tx(true)?;
            let b = tx.get_bucket("abc")?;
            
            println!("A: {:#?}", b.get(1_u32.to_be_bytes()));
            b.put(1_u32.to_be_bytes(), "*11111111111*")?;
            // println!("B: {:#?}", b.get(1_u32.to_be_bytes()));
            tx.commit()?;
        }
        println!("RO: {:#?}", ro_b.get(1_u32.to_be_bytes()));
    }

    Ok(())
}
