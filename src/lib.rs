use crossbeam::channel::{self, Receiver, Sender};
use dashmap::DashMap;
use std::collections::HashMap;
use std::ops::Range;
use std::path::{Path, PathBuf};
use tokio::task;

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

pub enum ChunkRequest {
    Download(DataChunk),
    Delete(ChunkId),
}

pub struct SimpleDataManager {
    data_dir: PathBuf,
    chunks: DashMap<ChunkId, DataChunk>, // Lock-free data structure for storing downloaded chunks
    request_senders: DashMap<ChunkId, Sender<ChunkRequest>>, // Channels for chunk requests
}

impl SimpleDataManager {
    pub fn new(data_dir: PathBuf) -> Self {
        let chunks = DashMap::new();
        let request_senders = DashMap::new();

        SimpleDataManager {
            data_dir,
            chunks,
            request_senders,
        }
    }

    fn handle_request(&self, chunk_id: ChunkId, request: ChunkRequest) {
        let sender = self.request_senders.entry(chunk_id).or_insert_with(|| {
            let (tx, rx) = channel::bounded(100);
            let chunk_id_clone = chunk_id.clone();
            let chunks_clone = self.chunks.clone();

            // Spawn a task to handle the specific chunk's requests
            task::spawn(async move {
                while let Ok(req) = rx.recv() {
                    match req {
                        ChunkRequest::Download(chunk) => {
                            // Simulate downloading the chunk
                            // ... download logic here ...

                            // After download, store the chunk
                            chunks_clone.insert(chunk.id, chunk);
                        }
                        ChunkRequest::Delete(id) => {
                            chunks_clone.remove(&id);
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

impl DataManager for SimpleDataManager {
    fn new(data_dir: PathBuf) -> Self {
        Self::new(data_dir)
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

// Worker struct to handle downloading
struct Worker {
    // Worker-specific fields
}

impl Worker {
    fn new() -> Self {
        Worker {
            // Initialize worker
        }
    }

    // Additional methods for worker functionality
}
