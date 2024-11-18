use std::path::PathBuf;

use async_trait::async_trait;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

use crate::errors::DataManagerError;

#[async_trait]
pub trait Storage {
    async fn store(&self, name: String, data: Vec<u8>) -> Result<(), DataManagerError>;
    async fn delete(&self, name: String) -> Result<(), DataManagerError>;
    async fn commit(self) -> Result<(), DataManagerError>;
    async fn rollback(self) -> Result<(), DataManagerError>;
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

#[async_trait]
impl Storage for LocalStorage {
    async fn store(&self, name: String, data: Vec<u8>) -> Result<(), DataManagerError> {
        File::open(self.temp_dir.join(name))
            .await?
            .write_all(&data)
            .await?;
        Ok(())
    }

    async fn delete(&self, name: String) -> Result<(), DataManagerError> {
        tokio::fs::remove_file(self.final_dir.join(name)).await?;
        Ok(())
    }

    async fn commit(self) -> Result<(), DataManagerError> {
        let mut dir = tokio::fs::read_dir(self.temp_dir.clone()).await?;
        while let Some(f) = dir.next_entry().await? {
            let ty = f.file_type().await?;
            if ty.is_file() {
                tokio::fs::rename(f.path(), self.final_dir.join(f.file_name())).await?
            }
        }
        tokio::fs::remove_dir(self.temp_dir).await?;
        Ok(())
    }

    async fn rollback(self) -> Result<(), DataManagerError> {
        tokio::fs::remove_dir(self.temp_dir).await?;
        Ok(())
    }
}
