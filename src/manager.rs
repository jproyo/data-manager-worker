use std::collections::HashMap;
use std::hash::Hash;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crossbeam::channel::{self, Sender};
use dashmap::DashMap;

use crate::downloader::Downloader;
use crate::errors::DataManagerError;
use crate::storage::{LocalStorage, Storage};

pub type DatasetId = [u8; 32];
pub type ChunkId = [u8; 32];

/// Data chunk description
#[derive(Clone)]
pub struct DataChunk {
    id: ChunkId,
    /// Dataset (blockchain) id
    dataset_id: DatasetId,
    /// Block range this chunk is responsible for (around 100 - 10000 blocks)
    block_range: Range<u64>,
    /// Data chunk files.
    /// A mapping between file names and HTTP URLs to download files from.
    /// Usually contains 1 - 10 files of various sizes.
    /// The total size of all files in the chunk is about 200 MB.
    files: HashMap<String, String>,
}

impl PartialEq for DataChunk {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.dataset_id == other.dataset_id
    }
}

impl std::fmt::Debug for DataChunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataChunk")
            .field("id", &self.id)
            .field("dataset_id", &self.dataset_id)
            .field("block_range", &self.block_range)
            .finish()
    }
}

pub trait DataManager: Send + Sync {
    fn new(data_dir: PathBuf) -> Self;
    fn download_chunk(&self, chunk: DataChunk);
    fn list_chunks(&self) -> Vec<ChunkId>;
    fn find_chunk(&self, dataset_id: [u8; 32], block_number: u64) -> Option<DataChunk>;
    fn delete_chunk(&self, chunk_id: ChunkId);
}

pub trait DataChunkRef: Send + Sync + Clone {
    fn path(&self) -> &Path;
}

enum ChunkRequest {
    Download(DataChunk),
    Delete(ChunkId),
}
#[derive(Clone)]
pub struct InMemoryDataManager<D> {
    chunks: Arc<DashMap<ChunkId, DataChunk>>,
    request_senders: Arc<DashMap<ChunkId, Sender<ChunkRequest>>>, // Channels for chunk requests
    downloader: D,
    path_dir: PathBuf,
}

impl<D> InMemoryDataManager<D>
where
    D: Downloader + Default + Clone + Send + Sync + 'static,
{
    pub fn create(path_dir: PathBuf, downloader: D) -> Self {
        let chunks = Arc::new(DashMap::new());
        let request_senders = Arc::new(DashMap::new());

        InMemoryDataManager {
            chunks,
            request_senders,
            downloader,
            path_dir,
        }
    }

    pub fn new_with_path(path_dir: PathBuf) -> Self {
        Self::create(path_dir, Default::default())
    }

    fn handle<F>(files: HashMap<String, String>, handle: F) -> Result<(), DataManagerError>
    where
        F: FnOnce(String, String) -> Result<(), DataManagerError> + Clone + Send + 'static,
    {
        let mut handles = vec![]; // Vector to store thread handles
        for (name, url) in files {
            // Spawn a new thread for each file
            let handle = handle.clone();
            let h = std::thread::spawn(move || {
                // Use a blocking call instead of async
                let result = handle(name, url);
                result
            });
            handles.push(h);
        }
        // Collect results from all threads
        for handle in handles {
            match handle.join() {
                Ok(_) => continue,
                Err(e) => {
                    return Err(DataManagerError::HandleFileError(
                        "Error handling file".to_string(),
                    ))
                }
            }
        }
        Ok(())
    }

    fn handle_request(&self, chunk_id: ChunkId, request: ChunkRequest) {
        let sender = self.request_senders.entry(chunk_id).or_insert_with(|| {
            let (tx, rx) = channel::bounded(100);
            let chunks_clone = self.chunks.clone();
            let path = self.path_dir.clone();
            let downloader = self.downloader.clone();

            // Spawn a thread to handle the specific chunk's requests
            std::thread::spawn(move || {
                while let Ok(req) = rx.recv() {
                    match req {
                        ChunkRequest::Download(chunk) => {
                            let storage = LocalStorage::new(path.clone());
                            let handle = {
                                let download = downloader.clone();
                                let storage = storage.clone();
                                move |name, url| {
                                    let data = download.download(url)?;
                                    storage.store(name, data)?;
                                    Ok(())
                                }
                            };
                            let result = Self::handle(chunk.files.clone(), handle);
                            match result {
                                Ok(_) => {
                                    if let Err(e) = storage.commit() {
                                        tracing::error!(
                                            "Error committing storage after downloading {e}"
                                        );
                                    } else {
                                        chunks_clone.insert(chunk_id, chunk);
                                    }
                                }
                                Err(e) => {
                                    storage.rollback().unwrap_or_else(|_| {
                                        tracing::warn!("Something went wrong on rollback");
                                    });
                                    tracing::error!("Error downloading file {e}");
                                }
                            }
                        }
                        ChunkRequest::Delete(id) => {
                            let files = if let Some(entry) = chunks_clone.get(&id) {
                                entry.files.clone()
                            } else {
                                HashMap::new()
                            };
                            if !files.is_empty() {
                                let storage = LocalStorage::new(path.clone());
                                let handle = {
                                    let storage = storage.clone();
                                    move |name, _url| {
                                        storage.delete(name)?;
                                        Ok(())
                                    }
                                };
                                let result = Self::handle(files, handle);
                                match result {
                                    Ok(_) => {
                                        if let Err(e) = storage.commit() {
                                            tracing::error!(
                                                "Error committing storage after remove {e}"
                                            );
                                        } else {
                                            chunks_clone.remove(&chunk_id);
                                        }
                                    }
                                    Err(e) => {
                                        storage.rollback().unwrap_or_else(|_| {
                                            tracing::warn!("Something went wrong on rollback");
                                        });
                                        tracing::error!("Error removing file {e}");
                                    }
                                }
                            }
                        }
                    }
                }
            });
            tx
        });

        // Send the request to the appropriate channel
        let _ = sender.send(request);
    }
}

impl<D> DataManager for InMemoryDataManager<D>
where
    D: Downloader + Clone + Send + Sync + Default + 'static,
{
    fn new(data_dir: PathBuf) -> Self {
        InMemoryDataManager::new_with_path(data_dir)
    }

    fn download_chunk(&self, chunk: DataChunk) {
        self.handle_request(chunk.id, ChunkRequest::Download(chunk));
    }

    fn list_chunks(&self) -> Vec<ChunkId> {
        self.chunks.iter().map(|r| *r.key()).collect()
    }

    fn find_chunk(&self, dataset_id: [u8; 32], block_number: u64) -> Option<DataChunk> {
        self.chunks
            .iter()
            .find(|entry| {
                entry.value().dataset_id == dataset_id
                    && entry.value().block_range.contains(&block_number)
            })
            .map(|entry| entry.value().clone())
    }

    fn delete_chunk(&self, chunk_id: ChunkId) {
        self.handle_request(chunk_id, ChunkRequest::Delete(chunk_id));
    }
}
#[cfg(test)]
mod tests {
    use rand::random;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::{fmt, EnvFilter};

    use super::*;
    use crate::downloader::MockDownloader;
    use std::path::PathBuf;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_download_chunk() {
        let data_dir = PathBuf::from("test_data");
        let downloader = MockDownloader;
        let data_manager = InMemoryDataManager::create(data_dir.clone(), downloader);

        let id = random();
        let dataset_id = random();
        let chunk = DataChunk {
            id,
            dataset_id,
            block_range: 0..100,
            files: vec![("file1".to_string(), "file_url".to_string())]
                .into_iter()
                .collect(),
        };

        data_manager.download_chunk(chunk.clone());
        thread::sleep(Duration::from_secs(1));

        let stored_chunk = data_manager
            .find_chunk(chunk.dataset_id, chunk.block_range.start + 1)
            .expect("Chunk should be found");
        assert_eq!(stored_chunk, chunk);
    }

    #[test]
    fn test_stress_download_and_delete_chunks() {
        use rand::random;
        use std::sync::Arc;
        use std::thread;

        let data_dir = PathBuf::from("test_data");
        let downloader = MockDownloader;
        let data_manager = Arc::new(InMemoryDataManager::create(data_dir.clone(), downloader));
        let num_chunks = 10; // Number of chunks to create
        let num_operations = 5; // Number of download/delete operations per chunk

        let handles: Vec<_> = (0..num_chunks)
            .map(|_| {
                let data_manager = Arc::clone(&data_manager);
                thread::spawn(move || {
                    let id = random();
                    let dataset_id = random();
                    let chunk = DataChunk {
                        id,
                        dataset_id,
                        block_range: 0..100,
                        files: vec![("file1".to_string(), "file_url".to_string())]
                            .into_iter()
                            .collect(),
                    };

                    // Perform multiple downloads and deletions
                    for _ in 0..num_operations {
                        data_manager.download_chunk(chunk.clone());
                        data_manager.delete_chunk(chunk.id);
                    }
                })
            })
            .collect();

        // Wait for all threads to finish
        for handle in handles {
            handle.join().unwrap();
        }

        thread::sleep(Duration::from_secs(3));

        // Verify that no chunks remain in the data manager
        let remaining_chunks = data_manager.list_chunks();
        assert!(
            remaining_chunks.is_empty(),
            "Chunks should be empty after stress test"
        );
    }
}
