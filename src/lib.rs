use std::path::PathBuf;

use self::manager::{DataManager, InMemoryDataManager};

mod downloader;
pub mod errors;
pub mod manager;
mod storage;

// Worker struct to handle downloading
pub struct Worker<M> {
    data_manager: M,
}

impl<M> Worker<M>
where
    M: DataManager,
{
    pub fn new(path_dir: PathBuf) -> Self {
        Worker {
            data_manager: DataManager::new(path_dir),
        }
    }
}
