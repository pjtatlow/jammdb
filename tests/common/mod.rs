#[allow(dead_code)]
#[allow(clippy::mutable_key_type)]
pub mod record;
use std::path::Path;

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

impl AsRef<Path> for RandomFile {
    fn as_ref(&self) -> &Path {
        self.path.as_path()
    }
}

impl Drop for RandomFile {
    #[allow(unused_must_use)]
    fn drop(&mut self) {
        std::fs::remove_file(&self.path);
    }
}
