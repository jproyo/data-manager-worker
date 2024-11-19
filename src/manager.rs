use std::collections::HashMap;
use std::future::Future;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::task::JoinSet;

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

#[derive(Debug)]
struct ChunkState {
    chunk: DataChunk,
    state: State,
}

impl ChunkState {
    fn new(chunk: DataChunk) -> Self {
        Self {
            chunk,
            state: State::Present,
        }
    }

    fn is_deleted(&self) -> bool {
        self.state == State::Deleted
    }
}

#[derive(PartialEq, Debug)]
enum State {
    Present,
    Deleted,
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

#[derive(Debug)]
enum ChunkRequest {
    Download(DataChunk),
    Delete(ChunkId),
}
#[derive(Clone)]
pub struct InMemoryDataManager<D> {
    chunks: Arc<DashMap<ChunkId, ChunkState>>,
    request_senders: Arc<DashMap<ChunkId, UnboundedSender<ChunkRequest>>>, // Channels for chunk requests
    downloader: D,
    path_dir: PathBuf,
}

impl<D> InMemoryDataManager<D>
where
    D: Downloader + Default + Clone + Send + Sync + 'static,
{
    /// Create a new InMemoryDataManager with the given path directory and downloader.
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

    /// Handle the download and deletion requests for a chunk.
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
                Err(e) => return Err(DataManagerError::HandleFileError(e.to_string())),
            }
        }
        Ok(())
    }

    pub fn new_with_path(path_dir: PathBuf) -> Self {
        Self::create(path_dir, Default::default())
    }

    fn handle_request(&self, chunk_id: ChunkId, request: ChunkRequest) {
        let sender = self.request_senders.entry(chunk_id).or_insert_with(|| {
            let (tx, mut rx) = mpsc::unbounded_channel();
            let path = self.path_dir.clone();
            let downloader = self.downloader.clone();
            let chunks_clone = self.chunks.clone();
            // Spawn a task to handle the specific chunk's requests
            tokio::task::spawn(async move {
                loop {
                    tokio::select! {
                        Some(req) = rx.recv() => {
                            match req {
                                ChunkRequest::Download(chunk) => {
                                    if let Some(ref mut c) = chunks_clone.get_mut(&chunk_id) {
                                        if c.is_deleted() {
                                            c.state = State::Present;
                                        }
                                    }
                                    let storage = LocalStorage::new(path.clone());
                                    let handle = {
                                        let download = downloader.clone();
                                        let storage = storage.clone();
                                        move |name, url| async move {
                                            let data = download.download(url)?;
                                            storage.store(name, data)?;
                                            Ok(()) as Result<(), DataManagerError>
                                        }
                                    };

                                    let result = Self::handle(chunk.files.clone(), handle).await;
                                    match result {
                                        Ok(_) => {
                                            if let Err(e) = storage.commit() {
                                                tracing::error!(
                                                    "Error committing storage after downloading {e}"
                                                );
                                            } else {
                                                chunks_clone.insert(chunk_id, ChunkState::new(chunk));
                                                tracing::info!(
                                                    "Inserting chunk {:?}", 
                                                    chunk_id
                                                );
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
                                     if let Some(ref mut c) = chunks_clone.get_mut(&id) {
                                        if !c.is_deleted() {
                                            c.state = State::Deleted;
                                        }
                                    }
                                    let files = chunks_clone.get(&id).map(|x| x.chunk.files.clone()).unwrap_or_default();                                        
                                    let storage = LocalStorage::new(path.clone());
                                    let handle = {
                                        let storage = storage.clone();
                                        move |name, _url| async move {
                                            storage.delete(name)?;
                                            Ok(())
                                        }
                                    };
                                    let result = Self::handle(files, handle).await;
                                    match result {
                                        Ok(_) => {
                                            if let Err(e) = storage.commit() {
                                                tracing::error!(
                                                    "Error committing storage after remove {e}"
                                                );
                                            } else {
                                                chunks_clone.remove(&chunk_id);
                                                tracing::info!("Removing chunk {:?}", chunk_id);
                                            }
                                        },
                                        Err(e) => {
                                            storage.rollback().unwrap_or_else(|_| {
                                                tracing::warn!("Something went wrong on rollback");
                                            });
                                            tracing::error!("Error removing file {e}");
                                        }
                                    }
                            }
                        } 
                    } else => {
                            // Exit the loop if the channel is closed
                            break;
                        }
                    }
                }
            });
            tx
        });
        // Send the request to the appropriate channel
        sender.send(request).unwrap();
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
        self.chunks
            .iter()
            .filter(|x| !x.is_deleted())
            .map(|r| *r.key())
            .collect()
    }

    fn find_chunk(&self, dataset_id: [u8; 32], block_number: u64) -> Option<DataChunk> {
        self.chunks
            .iter()
            .find(|entry| {
                entry.chunk.dataset_id == dataset_id
                    && entry.chunk.block_range.contains(&block_number)
                    && !entry.is_deleted()
            })
            .map(|entry| entry.chunk.clone())
    }

    fn delete_chunk(&self, chunk_id: ChunkId) {
        self.handle_request(chunk_id, ChunkRequest::Delete(chunk_id));
    }
}
#[cfg(test)]
mod tests {
    use rand::{random, Rng};
    use tracing_subscriber::fmt;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::EnvFilter;

    use super::*;
    use crate::downloader::MockDownloader;
    use std::cmp::min;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicU64;
    use std::thread;
    use std::time::Duration;

    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn test_download_chunk() {
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

        let c = chunk.clone();
        let dm = data_manager.clone();
        tokio::task::spawn_blocking(move || dm.download_chunk(c));
        thread::sleep(Duration::from_secs(1));

        let stored_chunk = data_manager
            .find_chunk(chunk.dataset_id, chunk.block_range.start + 1)
            .expect("Chunk should be found");
        assert_eq!(stored_chunk, chunk);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn test_stress_download_and_delete_chunks() {
        let data_dir = PathBuf::from("test_data");
        let downloader = MockDownloader;
        let data_manager = InMemoryDataManager::create(data_dir.clone(), downloader);
        let num_chunks = 10; // Number of chunks to create
        let num_operations = 5; // Number of download/delete operations per chunk

        for i in 0..num_chunks {
            let data_manager = data_manager.clone();
            tokio::task::spawn_blocking(move || {
                let mut r = rand::thread_rng();
                let mut id: [u8; 32] = r.gen();
                id[0] = i + 1;
                let mut dataset_id: [u8; 32] = r.gen();
                dataset_id[0] = i + 1;
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
            });
        }

        thread::sleep(Duration::from_secs(3));

        // Verify that no chunks remain in the data manager
        let remaining_chunks = data_manager.list_chunks();
        assert!(
            remaining_chunks.is_empty(),
            "Chunks should be empty after stress test"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn test_stress_download_and_delete_chunks_without_ordering() {
        tracing_subscriber::registry()
            .with(fmt::layer())
            .with(EnvFilter::from_default_env())
            .init();
        let data_dir = PathBuf::from("test_data");
        let downloader = MockDownloader;
        let data_manager = InMemoryDataManager::create(data_dir.clone(), downloader);
        let num_chunks = 1_000; // Number of chunks to create

        let counter_delete = Arc::new(AtomicU64::new(0));
        let counter_download = Arc::new(AtomicU64::new(0));
        // Use the existing thread pool from InMemoryDataManager
        let mut joinset = JoinSet::new();
        for i in 0..num_chunks {
            let counter_delete = counter_delete.clone();
            let mut r = rand::thread_rng();
            let id = r.gen();
            let dataset_id = r.gen();
            let chunk = DataChunk {
                id,
                dataset_id,
                block_range: min(i, 100)..min(i + 10, 100_000),
                files: vec![("file1".to_string(), "file_url".to_string())]
                    .into_iter()
                    .collect(),
            };

            let data_manager = data_manager.clone();
            let data_manager_del = data_manager.clone();
            let chunk_id = chunk.id;
            let counter_download = counter_download.clone();
            joinset.spawn_blocking(move || {
                data_manager.download_chunk(chunk);
                counter_download.fetch_add(1, std::sync::atomic::Ordering::Release);
            });
            if i % 2 == 0 {
                joinset.spawn_blocking(move || {
                    counter_delete.fetch_add(1, std::sync::atomic::Ordering::Release);
                    data_manager_del.delete_chunk(chunk_id);
                });
            }
        }

        while let Some(r) = joinset.join_next().await {
            match r {
                Ok(_) => continue,
                Err(e) => unreachable!("error {}", e),
            }
        }

        thread::sleep(Duration::from_secs(5));

        // Verify that no chunks remain in the data manager
        let counter_delete = counter_delete.load(std::sync::atomic::Ordering::Acquire);
        let len_expected = data_manager.list_chunks().len();
        println!("len {} - real {}", len_expected, data_manager.chunks.len());
        assert!(1_000 > len_expected && len_expected >= counter_delete as usize);
    }
}