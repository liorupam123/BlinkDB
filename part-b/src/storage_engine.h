/**
 * @file storage_engine.h
 * @brief Header file for the LSM-based storage engine.
 * 
 * This file contains the declaration of the StorageEngine class and its supporting
 * components, including memtables, SSTables, bloom filters, LRU caches, and thread pools.
 */

#ifndef STORAGE_ENGINE_H
#define STORAGE_ENGINE_H

#include <string>
#include <map>
#include <vector>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <atomic>
#include <chrono>
#include <array>
#include <deque>
#include <functional>
#include <optional>
#include <unordered_map>
#include <list>
#include <thread>
#include <condition_variable>
#include <queue>
#include <stdexcept>
#include <cmath>

/// Number of shards for partitioned memtable
#define NUM_SHARDS 16

/// Number of read cache segments
#define NUM_CACHES 4

/**
 * @class BloomFilter
 * @brief Implements a bloom filter for efficient negative lookups.
 */
class BloomFilter {
public:
    /**
     * @brief Constructs a BloomFilter.
     * @param expected_items The expected number of items to store.
     * @param false_positive_rate The desired false positive rate.
     */
    BloomFilter(size_t expected_items = 10000, double false_positive_rate = 0.01) {
        bit_array_size_ = static_cast<size_t>(-expected_items * std::log(false_positive_rate) / (std::log(2) * std::log(2)));
        num_hashes_ = static_cast<size_t>(bit_array_size_ * std::log(2) / expected_items);
        
        bit_array_size_ = std::max(bit_array_size_, size_t(1024));
        num_hashes_ = std::max(num_hashes_, size_t(2));
        num_hashes_ = std::min(num_hashes_, size_t(20));
        
        bits_.resize(bit_array_size_, false);
    }

    /**
     * @brief Adds a key to the bloom filter.
     * @param key The key to add.
     */
    void add(const std::string& key) {
        size_t h1 = hash1(key);
        size_t h2 = hash2(key);
        
        for (size_t i = 0; i < num_hashes_; ++i) {
            size_t hash = (h1 + i * h2) % bit_array_size_;
            bits_[hash] = true;
        }
    }

    /**
     * @brief Checks if a key might be in the bloom filter.
     * @param key The key to check.
     * @return True if the key might be in the filter, false otherwise.
     */
    bool mightContain(const std::string& key) const {
        size_t h1 = hash1(key);
        size_t h2 = hash2(key);
        
        for (size_t i = 0; i < num_hashes_; ++i) {
            size_t hash = (h1 + i * h2) % bit_array_size_;
            if (!bits_[hash]) {
                return false;
            }
        }
        
        return true;
    }

private:
    size_t num_hashes_; ///< Number of hash functions.
    size_t bit_array_size_; ///< Size of the bit array.
    std::vector<bool> bits_; ///< The bit array.

    size_t hash1(const std::string& key) const {
        size_t hash = 14695981039346656037ULL;
        for (char c : key) {
            hash ^= static_cast<size_t>(c);
            hash *= 1099511628211ULL;
        }
        return hash;
    }

    size_t hash2(const std::string& key) const {
        size_t hash = 5381;
        for (char c : key) {
            hash = ((hash << 5) + hash) + static_cast<size_t>(c);
        }
        return hash;
    }
};

/**
 * @class LRUCache
 * @brief Implements an LRU (Least Recently Used) cache.
 * @tparam K The type of the cache keys.
 * @tparam V The type of the cache values.
 */
template <typename K, typename V>
class LRUCache {
public:
    /**
     * @brief Constructs an LRUCache.
     * @param capacity The maximum number of items the cache can hold.
     */
    explicit LRUCache(size_t capacity = 1000) : capacity_(capacity) {}
    
    // Delete copy constructor/assignment - mutex can't be copied
    LRUCache(const LRUCache&) = delete;
    LRUCache& operator=(const LRUCache&) = delete;
    
    // We don't need move operations as we'll initialize the cache in-place
    LRUCache(LRUCache&&) = delete;
    LRUCache& operator=(LRUCache&&) = delete;

    /**
     * @brief Retrieves a value from the cache.
     * @param key The key to retrieve.
     * @return The value if found, or std::nullopt if not found.
     */
    std::optional<V> get(const K& key) {
        std::lock_guard<std::mutex> lock(mutex_);
        
        auto it = cache_map_.find(key);
        if (it == cache_map_.end()) {
            return std::nullopt;
        }

        // Move to front (most recently used)
        cache_list_.splice(cache_list_.begin(), cache_list_, it->second);
        return it->second->second;
    }

    /**
     * @brief Inserts a key-value pair into the cache.
     * @param key The key to insert.
     * @param value The value to associate with the key.
     */
    void put(const K& key, const V& value) {
        std::lock_guard<std::mutex> lock(mutex_);
        
        auto it = cache_map_.find(key);
        if (it != cache_map_.end()) {
            // Update existing item and move to front
            it->second->second = value;
            cache_list_.splice(cache_list_.begin(), cache_list_, it->second);
            return;
        }

        // Remove least recently used item if full
        if (cache_list_.size() >= capacity_) {
            auto last = cache_list_.back();
            cache_map_.erase(last.first);
            cache_list_.pop_back();
        }

        // Insert new item at front
        cache_list_.emplace_front(key, value);
        cache_map_[key] = cache_list_.begin();
    }

private:
    using ListItem = std::pair<K, V>;
    using ListIterator = typename std::list<ListItem>::iterator;

    std::list<ListItem> cache_list_; ///< List to maintain LRU order.
    std::unordered_map<K, ListIterator> cache_map_; ///< Map for quick lookup.
    size_t capacity_; ///< Maximum capacity of the cache.
    std::mutex mutex_; ///< Mutex for thread safety.
};

/**
 * @class ThreadPool
 * @brief Implements a thread pool for background operations.
 */
class ThreadPool {
public:
    /**
     * @brief Constructs a ThreadPool.
     * @param num_threads The number of threads in the pool.
     */
    ThreadPool(size_t num_threads) : stop_(false) {
        for(size_t i = 0; i < num_threads; ++i) {
            workers_.emplace_back([this] {
                while(true) {
                    std::function<void()> task;
                    {
                        std::unique_lock<std::mutex> lock(queue_mutex_);
                        condition_.wait(lock, [this] { 
                            return stop_ || !tasks_.empty(); 
                        });
                        
                        if(stop_ && tasks_.empty()) {
                            return;
                        }
                        
                        task = std::move(tasks_.front());
                        tasks_.pop();
                    }
                    task();
                }
            });
        }
    }

    /**
     * @brief Enqueues a task into the thread pool.
     * @param task The task to enqueue.
     */
    void enqueue(std::function<void()> task) {
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            if(stop_) {
                throw std::runtime_error("enqueue on stopped ThreadPool");
            }
            tasks_.emplace(std::move(task));
        }
        condition_.notify_one();
    }

    /**
     * @brief Destructor for the ThreadPool.
     */
    ~ThreadPool() {
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            stop_ = true;
        }
        condition_.notify_all();
        for(std::thread &worker: workers_) {
            if(worker.joinable()) {
                worker.join();
            }
        }
    }

private:
    std::vector<std::thread> workers_; ///< Worker threads.
    std::queue<std::function<void()>> tasks_; ///< Task queue.
    std::mutex queue_mutex_; ///< Mutex for task queue.
    std::condition_variable condition_; ///< Condition variable for task notification.
    std::atomic<bool> stop_; ///< Flag to stop the thread pool.
};

/**
 * @class StorageEngine
 * @brief Implements the LSM-based storage engine.
 */
class StorageEngine {
public:
    /**
     * @brief Constructs a StorageEngine.
     * @param max_memory_size The maximum memory size allowed for the storage engine.
     * @param memtable_size The size threshold for the memtable before flushing.
     */
    StorageEngine(size_t max_memory_size = 1024 * 1024 * 100, 
                 size_t memtable_size = 1024 * 1024 * 10);

    /**
     * @brief Destructor for the StorageEngine.
     */
    ~StorageEngine();
    
    /**
     * @brief Inserts or updates a key-value pair in the storage engine.
     * @param key The key to insert or update.
     * @param value The value to associate with the key.
     * @return True if the operation is successful, false otherwise.
     */
    bool set(const std::string& key, const std::string& value);

    /**
     * @brief Retrieves the value associated with a key.
     * @param key The key to retrieve.
     * @param value The retrieved value.
     * @return True if the key exists, false otherwise.
     */
    bool get(const std::string& key, std::string& value);

    /**
     * @brief Deletes a key from the storage engine.
     * @param key The key to delete.
     * @return True if the operation is successful, false otherwise.
     */
    bool del(const std::string& key);
    
    /**
     * @brief Inserts multiple key-value pairs in the storage engine.
     * @param kvs The key-value pairs to insert.
     * @return True if the operation is successful, false otherwise.
     */
    bool multiSet(const std::vector<std::pair<std::string, std::string>>& kvs);

    /**
     * @brief Retrieves multiple values associated with keys.
     * @param keys The keys to retrieve.
     * @param results The retrieved key-value pairs.
     * @return True if the operation is successful, false otherwise.
     */
    bool multiGet(const std::vector<std::string>& keys, std::vector<std::pair<std::string, std::optional<std::string>>>& results);
    
    /**
     * @brief Gets the current memory usage of the storage engine.
     * @return The current memory usage.
     */
    size_t getMemoryUsage() const;

private:
    // Token bucket for flow control
    class TokenBucket {
    public:
        TokenBucket(size_t rate, size_t capacity) 
            : tokens_(capacity), rate_(rate), capacity_(capacity),
              last_refill_(std::chrono::steady_clock::now()) {}
        
        bool consumeToken() {
            std::lock_guard<std::mutex> lock(mutex_);
            
            refillTokens();
            
            if (tokens_ == 0) {
                return false;
            }
            
            tokens_--;
            return true;
        }
        
        void refillTokens() {
            auto now = std::chrono::steady_clock::now();
            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - last_refill_).count();
            
            if (elapsed > 0) {
                tokens_ = std::min(capacity_, tokens_ + (elapsed * rate_));
                last_refill_ = now;
            }
        }
        
    private:
        size_t tokens_;
        size_t rate_;
        size_t capacity_;
        std::chrono::steady_clock::time_point last_refill_;
        std::mutex mutex_;
    };

    // Segmented bloom filter
    class SegmentedBloomFilter {
    public:
        SegmentedBloomFilter(size_t expected_items = 10000, size_t num_segments = 4) {
            size_t items_per_segment = (expected_items + num_segments - 1) / num_segments;
            
            for (size_t i = 0; i < num_segments; i++) {
                segments_.emplace_back(items_per_segment);
            }
        }
        
        void add(const std::string& key) {
            size_t segment = getSegment(key);
            segments_[segment].add(key);
        }
        
        bool mightContain(const std::string& key) const {
            size_t segment = getSegment(key);
            return segments_[segment].mightContain(key);
        }
        
    private:
        std::vector<BloomFilter> segments_;
        
        size_t getSegment(const std::string& key) const {
            if (segments_.empty()) return 0;
            
            uint32_t hash = 0;
            for (char c : key) {
                hash = hash * 31 + c;
            }
            
            return hash % segments_.size();
        }
    };

    // Fence pointers for SSTable indexing
    class FencePointers {
    public:
        std::vector<std::string> keys;
        std::vector<size_t> offsets;
        
        // Binary search to find position
        size_t findPosition(const std::string& key) const {
            if (keys.empty()) return 0;
            
            // Binary search to find the position
            auto it = std::upper_bound(keys.begin(), keys.end(), key);
            if (it == keys.begin()) return 0;
            
            size_t index = std::distance(keys.begin(), it) - 1;
            return offsets[index];
        }
    };

    // Key prefix optimization
    class KeyPrefix {
    public:
        std::string prefix;
        std::unordered_map<std::string, size_t> suffix_to_index;
        
        void addKey(const std::string& key, size_t& common_prefix_len) {
            if (prefix.empty()) {
                prefix = key;
                common_prefix_len = key.length();
                return;
            }
            
            // Find common prefix length
            size_t min_len = std::min(prefix.length(), key.length());
            common_prefix_len = 0;
            
            while (common_prefix_len < min_len && prefix[common_prefix_len] == key[common_prefix_len]) {
                common_prefix_len++;
            }
            
            // Update prefix
            if (common_prefix_len < prefix.length()) {
                prefix = prefix.substr(0, common_prefix_len);
            }
            
            // Add suffix
            std::string suffix = key.substr(common_prefix_len);
            suffix_to_index[suffix] = suffix_to_index.size();
        }
        
        std::string getSuffix(const std::string& key) const {
            if (prefix.empty() || key.length() < prefix.length()) {
                return key;
            }
            
            // Check if key starts with prefix
            for (size_t i = 0; i < prefix.length(); i++) {
                if (key[i] != prefix[i]) {
                    return key;
                }
            }
            
            return key.substr(prefix.length());
        }
    };

    // Memtable shard
    struct MemTableShard {
        std::map<std::string, std::string> data;
        size_t size = 0;
        std::chrono::steady_clock::time_point creation_time;
        mutable std::shared_mutex mutex;  // Changed to mutable to allow locking in const methods
        
        MemTableShard() : creation_time(std::chrono::steady_clock::now()) {}
        
        void insert(const std::string& key, const std::string& value) {
            std::unique_lock<std::shared_mutex> lock(mutex);
            
            size_t old_size = 0;
            auto it = data.find(key);
            if (it != data.end()) {
                old_size = key.size() + it->second.size() + sizeof(size_t) * 2;
            }
            
            data[key] = value;
            
            size_t new_size = key.size() + value.size() + sizeof(size_t) * 2;
            size += (new_size - old_size);
        }
        
        bool get(const std::string& key, std::string& value) const {
            std::shared_lock<std::shared_mutex> lock(mutex);  // Now works with mutable mutex
            
            auto it = data.find(key);
            if (it != data.end()) {
                if (it->second.empty()) {
                    return false;
                }
                value = it->second;
                return true;
            }
            return false;
        }
        
        bool remove(const std::string& key) {
            std::unique_lock<std::shared_mutex> lock(mutex);
            
            auto it = data.find(key);
            if (it != data.end()) {
                size_t entry_size = key.size() + it->second.size() + sizeof(size_t) * 2;
                size -= entry_size;
                data.erase(it);
                return true;
            }
            return false;
        }
        
        size_t memoryUsage() const {
            std::shared_lock<std::shared_mutex> lock(mutex);  // Now works with mutable mutex
            return size;
        }
        
        bool empty() const {
            std::shared_lock<std::shared_mutex> lock(mutex);  // Now works with mutable mutex
            return data.empty();
        }
    };

    // Partitioned memtable
    struct PartitionedMemTable {
        std::array<MemTableShard, NUM_SHARDS> shards;
        
        size_t getShard(const std::string& key) const {
            uint32_t hash = 0;
            for (char c : key) {
                hash = hash * 31 + c;
            }
            return hash % shards.size();
        }
        
        void insert(const std::string& key, const std::string& value) {
            size_t shard_idx = getShard(key);
            shards[shard_idx].insert(key, value);
        }
        
        bool get(const std::string& key, std::string& value) const {
            size_t shard_idx = getShard(key);
            return shards[shard_idx].get(key, value);
        }
        
        bool remove(const std::string& key) {
            size_t shard_idx = getShard(key);
            return shards[shard_idx].remove(key);
        }
        
        size_t memoryUsage() const {
            size_t total = 0;
            for (const auto& shard : shards) {
                total += shard.memoryUsage();
            }
            return total;
        }
        
        bool empty() const {
            for (const auto& shard : shards) {
                if (!shard.empty()) {
                    return false;
                }
            }
            return true;
        }
    };

    // SSTable with optimization
    struct SSTable {
        std::map<std::string, std::string> data;
        SegmentedBloomFilter bloom_filter;
        FencePointers fence_pointers;
        KeyPrefix key_prefix;
        size_t level = 0;
        std::string min_key;
        std::string max_key;
        mutable std::atomic<size_t> access_count = {0};  // Changed to mutable
        
        SSTable() = default;
        
        explicit SSTable(const std::map<std::string, std::string>& source_data, size_t expected_items = 10000) 
            : bloom_filter(expected_items) {
            
            if (!source_data.empty()) {
                data = source_data;
                min_key = source_data.begin()->first;
                max_key = source_data.rbegin()->first;
                
                // Build fence pointers and optimize key prefixes
                buildFencePointers();
                
                // Add all keys to bloom filter
                for (const auto& [key, _] : source_data) {
                    bloom_filter.add(key);
                }
            }
        }
        
        bool get(const std::string& key, std::string& value) const {
            // Increment access count for this SSTable - now works with mutable atomic
            access_count++;
            
            // Use bloom filter for fast negative lookups
            if (!bloom_filter.mightContain(key)) {
                return false;
            }
            
            // Use key range for quick rejection
            if (!min_key.empty() && !max_key.empty()) {
                if (key < min_key || key > max_key) {
                    return false;
                }
            }
            
            // Actual lookup
            auto it = data.find(key);
            if (it == data.end()) {
                return false;
            }
            
            if (it->second.empty()) {
                // Tombstone value
                return false;
            }
            
            value = it->second;
            return true;
        }
        
        bool mightContain(const std::string& key) const {
            // Key range check for quick rejection
            if (!min_key.empty() && !max_key.empty()) {
                if (key < min_key || key > max_key) {
                    return false;
                }
            }
            
            return bloom_filter.mightContain(key);
        }
        
        size_t memoryUsage() const {
            size_t usage = 0;
            
            // Size of keys and values
            for (const auto& [key, value] : data) {
                usage += key.size() + value.size() + sizeof(key) + sizeof(value);
            }
            
            // Size of the map structure itself
            usage += sizeof(data) + (data.size() * (sizeof(std::map<std::string, std::string>::node_type)));
            
            return usage;
        }
        
        void buildFencePointers() {
            if (data.empty()) return;
            
            const size_t FENCE_INTERVAL = 16; // Every 16 keys
            
            size_t count = 0;
            size_t offset = 0;
            
            for (const auto& [key, _] : data) {
                if (count % FENCE_INTERVAL == 0) {
                    fence_pointers.keys.push_back(key);
                    fence_pointers.offsets.push_back(offset);
                }
                count++;
                offset++;
            }
        }
    };

    // Core methods
    void writeLogEntry(const std::string& operation, const std::string& key, const std::string& value = "");
    void flushMemTable(size_t shard_index = NUM_SHARDS);
    void compactLevel(size_t level);
    void checkAndScheduleCompaction();
    void recoverFromLog();
    void monitorAndAdjustCompaction();
    
    // Calculate shard index for a key
    size_t getShardIndex(const std::string& key) const;

    // Fast hash function for partitioning
    uint32_t murmurHash(const std::string& key) const {
        const uint32_t m = 0x5bd1e995;
        const int r = 24;
        uint32_t h = 0; // Seed

        // Mix 4 bytes at a time into the hash
        const unsigned char* data = (const unsigned char*)key.data();
        size_t len = key.size();

        while (len >= 4) {
            uint32_t k = *(uint32_t*)data;

            k *= m;
            k ^= k >> r;
            k *= m;

            h *= m;
            h ^= k;

            data += 4;
            len -= 4;
        }

        // Handle the last few bytes
        switch (len) {
            case 3: h ^= data[2] << 16; // FALLTHROUGH
            case 2: h ^= data[1] << 8;  // FALLTHROUGH
            case 1: h ^= data[0];
                    h *= m;
        };

        // Do a few final mixes
        h ^= h >> 13;
        h *= m;
        h ^= h >> 15;

        return h;
    }

    // Data members
    std::unique_ptr<PartitionedMemTable> active_memtable_; ///< Active memtable.
    std::vector<std::unique_ptr<PartitionedMemTable>> immutable_memtables_; ///< Immutable memtables.
    std::vector<std::vector<std::unique_ptr<SSTable>>> sstable_levels_; ///< SSTable levels.
    
    // LRU cache for hot items with multiple segments for less contention
    std::array<LRUCache<std::string, std::string>, NUM_CACHES> read_caches_; ///< LRU caches for hot items.
    
    // Thread pool for background operations
    ThreadPool thread_pool_; ///< Thread pool for background operations.
    
    // Token bucket for limiting background operations
    TokenBucket token_bucket_; ///< Token bucket for flow control.
    
    // Configuration parameters
    size_t max_memory_size_; ///< Maximum memory size allowed.
    size_t memtable_size_threshold_; ///< Memtable size threshold before flushing.
    std::atomic<size_t> current_memory_usage_ = {0}; ///< Current memory usage.
    
    // Adaptive compaction parameters
    std::atomic<size_t> reads_since_compaction_ = {0}; ///< Reads since the last compaction.
    std::atomic<size_t> writes_since_compaction_ = {0}; ///< Writes since the last compaction.
    std::chrono::steady_clock::time_point last_compaction_time_; ///< Last compaction time.
    std::atomic<float> compaction_frequency_ = {1.0}; ///< Compaction frequency.
    
    std::string log_file_path_ = "wal.log"; ///< Path to the write-ahead log file.
    mutable std::mutex log_mutex_; ///< Mutex for log operations.
    
    std::atomic<bool> flush_in_progress_ = {false}; ///< Flag indicating if a flush is in progress.
    std::atomic<bool> compaction_in_progress_ = {false}; ///< Flag indicating if a compaction is in progress.
    std::atomic<bool> shutdown_requested_ = {false}; ///< Flag indicating if a shutdown is requested.
    
    mutable std::shared_mutex rw_mutex_; ///< Read-write mutex for thread safety.
    std::mutex flush_mutex_; ///< Mutex for flush operations.
    std::mutex compaction_mutex_; ///< Mutex for compaction operations.
};

#endif // STORAGE_ENGINE_H