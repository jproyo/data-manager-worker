use std::io;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum DataManagerError {
    #[error("Error downloading file")]
    DownloadError,
    #[error("Error storing file")]
    StoreFileError(#[from] io::Error),
    #[error("Error handling file {0}")]
    HandleFileError(String),
}
