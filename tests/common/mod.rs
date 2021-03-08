use rand::{distributions::Alphanumeric, Rng};

pub struct RandomFile {
    pub path: std::path::PathBuf,
}

impl RandomFile {
    pub fn new() -> RandomFile {
        loop {
            let filename: String = std::str::from_utf8(
                rand::thread_rng()
                    .sample_iter(&Alphanumeric)
                    .take(30)
                    .collect::<Vec<u8>>()
                    .as_slice(),
            )
            .unwrap()
            .into();
            let path = std::env::temp_dir().join(filename);
            if path.metadata().is_err() {
                return RandomFile { path };
            }
        }
    }
}

impl Drop for RandomFile {
    #[allow(unused_must_use)]
    fn drop(&mut self) {
        std::fs::remove_file(&self.path);
    }
}
