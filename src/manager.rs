use std::collections::HashMap;
use std::future::Future;
use std::ops::Range;
use std::path::{Path, PathBuf};

use crossbeam::channel::{self, Sender};
use dashmap::DashMap;
use tokio::task::{self, JoinSet};

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
    chunks: DashMap<ChunkId, DataChunk>, // Lock-free data structure for storing downloaded chunks
    request_senders: DashMap<ChunkId, Sender<ChunkRequest>>, // Channels for chunk requests
    downloader: D,
    path_dir: PathBuf,
}

impl<D> InMemoryDataManager<D>
where
    D: Downloader + Clone + Send + Sync + 'static,
{
    pub fn new(path_dir: PathBuf, downloader: D) -> Self {
        let chunks = DashMap::new();
        let request_senders = DashMap::new();

        InMemoryDataManager {
            chunks,
            request_senders,
            downloader,
            path_dir,
        }
    }

    async fn handle<F, Fut>(
        files: HashMap<String, String>,
        handle: F,
    ) -> Result<(), DataManagerError>
    where
        F: FnOnce(String, String) -> Fut + Send + Clone + 'static,
        Fut: Future<Output = Result<(), DataManagerError>> + Send,
    {
        let mut set: JoinSet<Result<(), DataManagerError>> = JoinSet::new();
        for (name, url) in files {
            let h = handle.clone();
            set.spawn(async move {
                h(name, url).await?;
                Ok(())
            });
        }
        while let Some(r) = set.join_next().await {
            match r {
                Ok(Ok(_)) => {
                    continue;
                }
                Ok(Err(e)) => return Err(e),
                Err(e) => return Err(DataManagerError::HandleFileError(e)),
            }
        }
        Ok(())
    }

    fn handle_request(&self, chunk_id: ChunkId, request: ChunkRequest) {
        let sender = self.request_senders.entry(chunk_id).or_insert_with(|| {
            let (tx, rx) = channel::bounded(100);
            let chunk_id_clone = chunk_id.clone();
            let chunks_clone = self.chunks.clone();

            // Spawn a task to handle the specific chunk's requests
            let path = self.path_dir.clone();
            let downloader = self.downloader.clone();
            task::spawn(async move {
                while let Ok(req) = rx.recv() {
                    match req {
                        ChunkRequest::Download(chunk) => {
                            let storage = LocalStorage::new(path.clone());
                            let handle = {
                                let download = downloader.clone();
                                let storage = storage.clone();
                                |name, url| async move {
                                    let data = download.download(url).await?;
                                    storage.store(name, data).await?;
                                    Ok(())
                                }
                            };
                            let result = Self::handle(chunk.files.clone(), handle).await;
                            match result {
                                Ok(_) => match storage.commit().await {
                                    Ok(_) => {
                                        chunks_clone.insert(chunk_id_clone, chunk);
                                    }
                                    Err(e) => tracing::error!(
                                        "Error committing storage after downloading {e}"
                                    ),
                                },
                                Err(e) => {
                                    storage.rollback().await.unwrap_or(tracing::warn!(
                                        "Something went wrong on rollback"
                                    ));
                                    tracing::error!("Error downloading file {e}")
                                }
                            }
                        }
                        ChunkRequest::Delete(id) => {
                            if let Some(entry) = chunks_clone.get(&id) {
                                let storage = LocalStorage::new(path.clone());
                                let handle = {
                                    let storage = storage.clone();
                                    |name, _url| async move {
                                        storage.delete(name).await?;
                                        Ok(())
                                    }
                                };
                                let result = Self::handle(entry.files.clone(), handle).await;
                                match result {
                                    Ok(_) => match storage.commit().await {
                                        Ok(_) => {
                                            chunks_clone.remove(&chunk_id_clone);
                                        }
                                        Err(e) => tracing::error!(
                                            "Error commiting storage after remove {e}"
                                        ),
                                    },
                                    Err(e) => {
                                        storage.rollback().await.unwrap_or(tracing::warn!(
                                            "Something went wrong on rollback"
                                        ));
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
        let downloader = Default::default();
        InMemoryDataManager::new(data_dir, downloader)
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
