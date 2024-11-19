# In-Memory Data Manager

## Overview

The In-Memory Data Manager is designed to efficiently manage data chunks in a concurrent environment using Rust's async capabilities. This system is built to handle download and deletion requests for data chunks while ensuring that operations are non-blocking and efficient.

## Design Strategy

### 1. Channel per Chunk ID

Each chunk is associated with its own channel, allowing for dedicated handling of requests (either download or delete) for that specific chunk. This design choice ensures that requests for different chunks do not interfere with each other, enabling concurrent processing without contention. Each request is processed in its own thread, which allows for efficient handling of multiple operations simultaneously.

### 2. Tokio for Concurrency

The system leverages the Tokio runtime, a powerful asynchronous runtime for Rust. By using `async` and `await`, the In-Memory Data Manager can handle multiple tasks concurrently without blocking the main thread. This allows for efficient resource utilization and responsiveness, especially when dealing with I/O-bound operations such as downloading files.

### 3. Intermediate State Management

To optimize performance, the system maintains an intermediate state for each chunk, indicating whether it is present or deleted. This state management allows the system to quickly determine the status of a chunk without needing to perform additional checks or operations, thereby speeding up processing times and reducing the likelihood of blocking operations.

### 4. Stress Testing

The implementation includes comprehensive stress tests to ensure the robustness and reliability of the data manager under high load. These tests simulate multiple concurrent download and delete operations, verifying that the system can handle a large number of requests without failure. The results of these tests help identify potential bottlenecks and ensure that the system behaves as expected under stress.

## Future Work / TODOs

1. **Implementation of Storage**: Develop a concrete storage solution to persist data chunks beyond the in-memory representation. This will allow for data recovery and persistence across application restarts.

2. **Implementation of Downloader**: Create a downloader component that can handle real file downloads from specified URLs. This component should integrate seamlessly with the existing architecture.

3. **Non-Blocking Downloads and Storage**: Refactor the system to send real download and storage operations to separate channels or threads. This will further enhance the non-blocking nature of the application, allowing for even greater concurrency and responsiveness.

## Conclusion

The In-Memory Data Manager is a robust solution for managing data chunks in a concurrent environment. By utilizing channels, Tokio, and efficient state management, the system is designed to handle high loads and provide reliable performance. Future enhancements will focus on implementing persistent storage and improving the overall architecture for even better performance.