use crate::errors::DataManagerError;
use std::path::PathBuf;

pub trait Storage {
    fn store(&self, name: String, data: Vec<u8>) -> Result<(), DataManagerError>;
    fn delete(&self, name: String) -> Result<(), DataManagerError>;
    fn commit(self) -> Result<(), DataManagerError>;
    fn rollback(self) -> Result<(), DataManagerError>;
}

#[derive(Clone)]
pub struct LocalStorage {
    temp_dir: PathBuf,
    final_dir: PathBuf,
}

impl LocalStorage {
    pub fn new(path_dir: PathBuf) -> Self {
        let random: [u8; 32] = rand::random();
        let temp_dir = path_dir.join(format!("temp_{}", hex::encode(random)));
        Self {
            temp_dir,
            final_dir: path_dir.join("committed"),
        }
    }
}

impl Storage for LocalStorage {
    fn store(&self, name: String, data: Vec<u8>) -> Result<(), DataManagerError> {
        Ok(())
    }

    fn delete(&self, name: String) -> Result<(), DataManagerError> {
        Ok(())
    }

    fn commit(self) -> Result<(), DataManagerError> {
        Ok(())
    }

    fn rollback(self) -> Result<(), DataManagerError> {
        Ok(())
    }
}
