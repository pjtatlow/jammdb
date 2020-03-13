use rand::{Rng, distributions::Alphanumeric};


pub struct RandomFile {
	pub path: std::path::PathBuf,
}

impl RandomFile {
	pub fn new() -> RandomFile {
		loop {
			let filename: String = rand::thread_rng()
				.sample_iter(&Alphanumeric)
				.take(30)
				.collect();
			let path = std::env::temp_dir().join(filename);
			if let Err(_) =  path.metadata() {
				return RandomFile{path};
			}
		}
	}
}

impl Drop for RandomFile {
	#[allow(unused_must_use)]
	fn drop(&mut self) {
		println!("{:?}", self.path);
        std::fs::remove_file(&self.path);
    }
}