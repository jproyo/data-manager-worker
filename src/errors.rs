use std::io;

use thiserror::Error;
use tokio::task;

#[derive(Debug, Error)]
pub enum DataManagerError {
    #[error("Error downloading file")]
    DownloadError,
    #[error("Error storing file")]
    StoreFileError(#[from] io::Error),
    #[error("Error handling file {0}")]
    HandleFileError(task::JoinError),
}
