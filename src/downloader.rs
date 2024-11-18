use async_trait::async_trait;

use crate::errors::DataManagerError;

#[async_trait]
pub trait Downloader {
    async fn download(&self, url: String) -> Result<Vec<u8>, DataManagerError>;
}
