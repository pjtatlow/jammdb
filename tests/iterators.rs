use jammdb::{Bucket, Data, Error, DB};
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
		let db = DB::open(&random_file.path)?;
		{
			let mut tx = db.tx(true)?;
			let mut b = tx.create_bucket("abc")?;
			{
				let mut random_fruits = Vec::from(fruits.as_slice());
				let mut rng = rand::thread_rng();
				random_fruits.shuffle(&mut rng);
				// randomly insert the fruits into the bucket
				for fruit in random_fruits.iter() {
					let index = fruits.binary_search(fruit).unwrap();
					b.put(fruit, index.to_string())?;
				}
			}
			check_cursor_starts(&fruits, &b);
			tx.commit()?;
		}
		{
			let mut tx = db.tx(false)?;
			let b = tx.get_bucket("abc")?;
			check_cursor_starts(&fruits, &b);
		}
	}
	{
		let db = DB::open(&random_file.path)?;
		{
			let mut tx = db.tx(false)?;
			let b = tx.get_bucket("abc")?;
			check_cursor_starts(&fruits, &b);
		}
		{
			let mut tx = db.tx(false)?;
			let b = tx.get_bucket("abc")?;
			println!("7 {} {:?}", fruits[7], &fruits[7..]);
			check_cursor("bl", &fruits[6..], &b, 6);
		}
		{
			let mut tx = db.tx(true)?;
			let mut b = tx.get_bucket("abc")?;
			b.put("zomato", fruits.len().to_string())?;
			fruits.push("zomato");
			check_cursor_starts(&fruits, &b);
			tx.commit()?;
		}
		{
			let mut tx = db.tx(false)?;
			let mut b = tx.get_bucket("abc")?;
			println!("7 {} {:?}", fruits[7], &fruits[7..]);
			check_cursor("bl", &fruits[6..], &mut b, 6);
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
			println!("KEY {}", std::str::from_utf8(kv.key()).unwrap());
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
