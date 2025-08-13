# BLINK DB – LSM Tree Storage Engine with Client-Server Architecture

## 📌 Overview
- BLINK DB is a high-performance, write-optimized key-value storage engine built on a Log-Structured Merge (LSM) Tree architecture.
- It is designed to handle write-heavy workloads while maintaining efficient reads, range queries, and scalability through a client-server model.
- This repository contains:
  1. The LSM Tree–based storage engine.
  2. A multi-threaded client-server architecture for data access.
  3. A response handler for structured communication.
  4. Performance optimizations for large-scale workloads.

## 🚀 Features

### Core Storage Engine (LSM Tree)
- **Write-Optimized:** In-memory buffering, sequential disk writes, and batched operations.
- **Efficient Reads:** LRU caching, sorted data structures, and key range filtering.
- **Persistence:** SSTables for immutable, sorted on-disk storage.
- **Compaction:** Background merging to remove duplicates and apply deletions.
- **Concurrency:** Fine-grained locking and lock-free reads where possible.

### Client-Server System
- **Client Module:** Sends requests and receives responses over sockets.
- **Server Module:** Multi-threaded request handling with storage engine integration.
- **Response Handler:** Formats and validates structured messages.
- **Storage Interface:** CRUD operations with persistence and recovery.

## 📂 Architecture

### High-Level Components
1. Client – Connects to server, sends commands, receives results.
2. Server – Processes requests, queries storage engine, and responds.
3. Response Handler – Serializes/deserializes data between client and server.
4. Storage Engine – LSM Tree implementation with MemTable, SSTables, and cache.
5. Compaction System – Background process for merging and cleaning data.

## 🔄 Data Flow

### Write Path
1. Client sends `SET key value` to server.
2. Server stores entry in LRU Cache and active MemTable.
3. When MemTable reaches size limit:
   - Flush to Level 0 SSTable.
   - Trigger compaction if necessary.

### Read Path
1. Client sends `GET key` to server.
2. Server checks:
   - LRU Cache
   - Active MemTable
   - Immutable MemTable
   - SSTables (binary search)
3. Most recent value is returned.

### Delete Path
1. Client sends `DEL key`.
2. Tombstone marker is written to MemTable.
3. Deleted data removed during compaction.

## ⚡ Performance Optimizations
- LRU Cache for frequently accessed items.
- Sequential I/O to minimize disk seek times.
- Binary Search on sorted vectors for fast lookups.
- Size-Tiered & Level-Based Compaction to reduce write amplification.
- Batch Processing for I/O efficiency.
- Lazy Loading to save memory.
- Compression to reduce disk usage.

## 🛡 Security & Reliability
- Input Validation to prevent injection attacks.
- Thread-Safe Storage Access with read-write locks.
- Planned TLS Encryption for secure communication.
- Planned Role-Based Access Control (RBAC) for permissions.
- Rate-Limiting to mitigate DoS attacks.
- Data Encryption for stored values.

## 🧵 Concurrency
- Thread Pools for efficient request handling.
- Dedicated Threads per Client for isolation.
- Condition Variables for synchronized storage access.
- Lock-Free Reads where possible.

## 📈 Future Enhancements
- Bloom Filters for faster key existence checks.
- Write-Ahead Log (WAL) for crash recovery.
- Multi-threaded Compaction for performance.
- RESTful API for third-party integrations.
- GUI for DB Management.
- Load Balancing & Failover for high availability.

## 📄 Detailed Project Report
- [Part A](https://drive.google.com/file/d/13OOTVfZ4FC2w6mIcKspP981Yf-Bxhlo4/view?usp=drive_link)
- [Part B](https://drive.google.com/file/d/1wu9fbfYfR1mAp_CY8LQBI4uVLxxigjLl/view?usp=drive_link)
