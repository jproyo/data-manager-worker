use crate::errors::DataManagerError;

pub trait Downloader {
    fn download(&self, url: String) -> Result<Vec<u8>, DataManagerError>;
}

#[derive(Default, Clone)]
pub struct MockDownloader;
impl Downloader for MockDownloader {
    fn download(&self, _url: String) -> Result<Vec<u8>, DataManagerError> {
        Ok(vec![]) // Placeholder implementation
    }
}
