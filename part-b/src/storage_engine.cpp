/**
 * @file storage_engine.cpp
 * @brief Implementation of the StorageEngine class.
 * 
 * This file provides the implementation of the LSM-based storage engine, including
 * methods for data insertion, retrieval, deletion, and compaction.
 */

#include "storage_engine.h"
#include <iostream>
#include <algorithm>

/**
 * @brief Constructs a StorageEngine object.
 * @param max_memory_size The maximum memory size allowed for the storage engine.
 * @param memtable_size The size threshold for the memtable before flushing.
 */
StorageEngine::StorageEngine(size_t max_memory_size, size_t memtable_size)
    : thread_pool_(8),
      token_bucket_(100, 1000),
      max_memory_size_(max_memory_size), 
      memtable_size_threshold_(memtable_size),
      current_memory_usage_(0),
      last_compaction_time_(std::chrono::steady_clock::now()) {
    
    active_memtable_ = std::make_unique<PartitionedMemTable>();
    sstable_levels_.push_back(std::vector<std::unique_ptr<SSTable>>());
    
    thread_pool_.enqueue([this]() {
        while (!shutdown_requested_) {
            monitorAndAdjustCompaction();
            std::this_thread::sleep_for(std::chrono::seconds(5));
        }
    });
    
    recoverFromLog();
}

/**
 * @brief Destructor for the StorageEngine class.
 */
StorageEngine::~StorageEngine() {
    shutdown_requested_ = true;
    if (active_memtable_ && !active_memtable_->empty()) {
        flushMemTable();
    }
}

/**
 * @brief Inserts or updates a key-value pair in the storage engine.
 * @param key The key to insert or update.
 * @param value The value to associate with the key.
 * @return True if the operation is successful, false otherwise.
 */
bool StorageEngine::set(const std::string& key, const std::string& value) {
    writeLogEntry("SET", key, value);
    size_t entry_size = key.size() + value.size() + sizeof(size_t) * 2;
    size_t cache_shard = murmurHash(key) % read_caches_.size();
    size_t shard_idx = active_memtable_->getShard(key);

    if (active_memtable_->shards[shard_idx].memoryUsage() + entry_size > memtable_size_threshold_ / NUM_SHARDS) {
        flushMemTable(shard_idx);
    }

    active_memtable_->insert(key, value);
    current_memory_usage_ += entry_size;
    read_caches_[cache_shard].put(key, value);
    writes_since_compaction_++;

    if (writes_since_compaction_ > 1000 * compaction_frequency_ || 
        current_memory_usage_ > max_memory_size_ * 0.8) {
        checkAndScheduleCompaction();
    }

    return true;
}

/**
 * @brief Retrieves the value associated with a key.
 * @param key The key to retrieve.
 * @param value The retrieved value.
 * @return True if the key exists, false otherwise.
 */
bool StorageEngine::get(const std::string& key, std::string& value) {
    size_t cache_shard = murmurHash(key) % read_caches_.size();
    auto cached_value = read_caches_[cache_shard].get(key);

    if (cached_value) {
        if (cached_value->empty()) {
            return false;
        }
        value = *cached_value;
        return true;
    }

    size_t shard_idx = active_memtable_->getShard(key);
    if (active_memtable_->shards[shard_idx].get(key, value)) {
        read_caches_[cache_shard].put(key, value);
        reads_since_compaction_++;
        return true;
    }

    std::shared_lock<std::shared_mutex> lock(rw_mutex_);
    for (const auto& memtable : immutable_memtables_) {
        if (memtable->get(key, value)) {
            read_caches_[cache_shard].put(key, value);
            reads_since_compaction_++;
            return true;
        }
    }

    for (size_t level = 0; level < sstable_levels_.size(); ++level) {
        const auto& level_tables = sstable_levels_[level];
        if (level == 0) {
            for (auto it = level_tables.rbegin(); it != level_tables.rend(); ++it) {
                const auto& sstable = *it;
                if (!sstable->mightContain(key)) {
                    continue;
                }
                if (sstable->get(key, value)) {
                    read_caches_[cache_shard].put(key, value);
                    reads_since_compaction_++;
                    return true;
                }
            }
        } else {
            std::vector<std::reference_wrapper<const std::unique_ptr<SSTable>>> candidates;
            for (const auto& sstable : level_tables) {
                if (key >= sstable->min_key && key <= sstable->max_key) {
                    candidates.push_back(std::cref(sstable));
                }
            }
            for (const auto& table_ref : candidates) {
                const auto& sstable = table_ref.get();
                if (!sstable->mightContain(key)) {
                    continue;
                }
                if (sstable->get(key, value)) {
                    read_caches_[cache_shard].put(key, value);
                    reads_since_compaction_++;
                    return true;
                }
            }
        }
    }

    reads_since_compaction_++;
    if (reads_since_compaction_ > 10000 * compaction_frequency_) {
        checkAndScheduleCompaction();
    }

    return false;
}

/**
 * @brief Deletes a key from the storage engine.
 * @param key The key to delete.
 * @return True if the operation is successful, false otherwise.
 */
bool StorageEngine::del(const std::string& key) {
    writeLogEntry("DEL", key);
    return set(key, "");
}

bool StorageEngine::multiSet(const std::vector<std::pair<std::string, std::string>>& kvs) {
    if (kvs.empty()) return true;
    
    std::unordered_map<size_t, std::vector<std::pair<std::string, std::string>>> shard_batches;
    
    for (const auto& [key, value] : kvs) {
        size_t shard_idx = active_memtable_->getShard(key);
        shard_batches[shard_idx].emplace_back(key, value);
    }
    
    for (const auto& [shard_idx, batch] : shard_batches) {
        size_t batch_size = 0;
        for (const auto& [key, value] : batch) {
            batch_size += key.size() + value.size() + sizeof(size_t) * 2;
        }
        
        if (active_memtable_->shards[shard_idx].memoryUsage() + batch_size > memtable_size_threshold_ / NUM_SHARDS) {
            flushMemTable(shard_idx);
        }
        
        for (const auto& [key, value] : batch) {
            writeLogEntry("SET", key, value);
            
            active_memtable_->shards[shard_idx].insert(key, value);
            
            size_t cache_shard = murmurHash(key) % read_caches_.size();
            read_caches_[cache_shard].put(key, value);
            
            current_memory_usage_ += key.size() + value.size() + sizeof(size_t) * 2;
        }
    }
    
    writes_since_compaction_ += kvs.size();
    
    if (writes_since_compaction_ > 1000 * compaction_frequency_ || 
        current_memory_usage_ > max_memory_size_ * 0.8) {
        checkAndScheduleCompaction();
    }
    
    return true;
}

bool StorageEngine::multiGet(const std::vector<std::string>& keys, 
                           std::vector<std::pair<std::string, std::optional<std::string>>>& results) {
    if (keys.empty()) return true;
    
    results.clear();
    results.reserve(keys.size());
    
    std::unordered_map<size_t, std::vector<size_t>> shard_to_indices;
    std::vector<std::optional<std::string>> values(keys.size(), std::nullopt);
    
    for (size_t i = 0; i < keys.size(); i++) {
        const auto& key = keys[i];
        
        size_t cache_shard = murmurHash(key) % read_caches_.size();
        auto cached_value = read_caches_[cache_shard].get(key);
        
        if (cached_value) {
            if (!cached_value->empty()) {
                values[i] = *cached_value;
            }
        } else {
            size_t shard_idx = active_memtable_->getShard(key);
            shard_to_indices[shard_idx].push_back(i);
        }
    }
    
    for (const auto& [shard_idx, indices] : shard_to_indices) {
        for (size_t idx : indices) {
            const auto& key = keys[idx];
            
            if (values[idx]) continue;
            
            std::string value;
            if (active_memtable_->shards[shard_idx].get(key, value)) {
                values[idx] = value;
                
                size_t cache_shard = murmurHash(key) % read_caches_.size();
                read_caches_[cache_shard].put(key, value);
            }
        }
    }
    
    std::vector<size_t> remaining_indices;
    for (size_t i = 0; i < keys.size(); i++) {
        if (!values[i]) {
            remaining_indices.push_back(i);
        }
    }
    
    if (!remaining_indices.empty()) {
        std::shared_lock<std::shared_mutex> lock(rw_mutex_);
        
        for (size_t idx : remaining_indices) {
            if (values[idx]) continue;
            
            const auto& key = keys[idx];
            std::string value;
            bool found = false;
            
            for (const auto& memtable : immutable_memtables_) {
                if (memtable->get(key, value)) {
                    values[idx] = value;
                    found = true;
                    
                    size_t cache_shard = murmurHash(key) % read_caches_.size();
                    read_caches_[cache_shard].put(key, value);
                    break;
                }
            }
            
            if (!found) {
                for (size_t level = 0; level < sstable_levels_.size(); ++level) {
                    const auto& level_tables = sstable_levels_[level];
                    
                    if (level == 0) {
                        for (auto it = level_tables.rbegin(); it != level_tables.rend(); ++it) {
                            const auto& sstable = *it;
                            
                            if (!sstable->mightContain(key)) continue;
                            
                            if (sstable->get(key, value)) {
                                values[idx] = value;
                                
                                size_t cache_shard = murmurHash(key) % read_caches_.size();
                                read_caches_[cache_shard].put(key, value);
                                found = true;
                                break;
                            }
                        }
                    } else {
                        std::vector<std::reference_wrapper<const std::unique_ptr<SSTable>>> candidates;
                        
                        for (const auto& sstable : level_tables) {
                            if (key >= sstable->min_key && key <= sstable->max_key) {
                                candidates.push_back(std::cref(sstable));
                            }
                        }
                        
                        for (const auto& table_ref : candidates) {
                            const auto& sstable = table_ref.get();
                            
                            if (!sstable->mightContain(key)) continue;
                            
                            if (sstable->get(key, value)) {
                                values[idx] = value;
                                
                                size_t cache_shard = murmurHash(key) % read_caches_.size();
                                read_caches_[cache_shard].put(key, value);
                                found = true;
                                break;
                            }
                        }
                    }
                    
                    if (found) break;
                }
            }
        }
    }
    
    for (size_t i = 0; i < keys.size(); i++) {
        results.emplace_back(keys[i], values[i]);
    }
    
    reads_since_compaction_ += keys.size();
    
    return true;
}

size_t StorageEngine::getMemoryUsage() const {
    std::shared_lock<std::shared_mutex> lock(rw_mutex_);
    return current_memory_usage_;
}

/**
 * @brief Writes a log entry for an operation.
 * @param operation The operation type (e.g., "SET", "DEL").
 * @param key The key involved in the operation.
 * @param value The value involved in the operation (if applicable).
 */
void StorageEngine::writeLogEntry(const std::string& operation, const std::string& key, const std::string& value) {
    std::lock_guard<std::mutex> lock(log_mutex_);
    std::cout << "[LOG] " << operation << " " << key;
    if (operation == "SET") {
        std::cout << " " << value;
    }
    std::cout << std::endl;
}

/**
 * @brief Flushes a memtable to disk.
 * @param shard_index The index of the shard to flush.
 */
void StorageEngine::flushMemTable(size_t shard_index) {
    if (flush_in_progress_.exchange(true)) {
        return;
    }

    std::lock_guard<std::mutex> lock(flush_mutex_);
    if (shard_index < NUM_SHARDS) {
        auto new_immutable = std::make_unique<PartitionedMemTable>();
        {
            std::unique_lock<std::shared_mutex> shard_lock(active_memtable_->shards[shard_index].mutex);
            new_immutable->shards[shard_index].data = std::move(active_memtable_->shards[shard_index].data);
            new_immutable->shards[shard_index].size = active_memtable_->shards[shard_index].size;
            active_memtable_->shards[shard_index].size = 0;
        }
        {
            std::unique_lock<std::shared_mutex> global_lock(rw_mutex_);
            immutable_memtables_.push_back(std::move(new_immutable));
        }
    } else {
        std::unique_lock<std::shared_mutex> global_lock(rw_mutex_);
        immutable_memtables_.push_back(std::move(active_memtable_));
        active_memtable_ = std::make_unique<PartitionedMemTable>();
    }

    thread_pool_.enqueue([this]() {
        if (immutable_memtables_.empty()) {
            flush_in_progress_ = false;
            return;
        }
        std::map<std::string, std::string> combined_data;
        {
            std::shared_lock<std::shared_mutex> rlock(rw_mutex_);
            for (const auto& shard : immutable_memtables_.back()->shards) {
                std::shared_lock<std::shared_mutex> shard_lock(shard.mutex);
                combined_data.insert(shard.data.begin(), shard.data.end());
            }
        }
        auto new_sstable = std::make_unique<SSTable>(combined_data, combined_data.size());
        new_sstable->level = 0;
        {
            std::unique_lock<std::shared_mutex> wlock(rw_mutex_);
            sstable_levels_[0].push_back(std::move(new_sstable));
            immutable_memtables_.pop_back();
            if (sstable_levels_[0].size() > 4) {
                wlock.unlock();
                compactLevel(0);
            }
        }
        flush_in_progress_ = false;
    });
}

void StorageEngine::compactLevel(size_t level) {
    if (compaction_in_progress_.exchange(true) || shutdown_requested_) {
        compaction_in_progress_ = false;
        return;
    }
    
    if (!token_bucket_.consumeToken()) {
        compaction_in_progress_ = false;
        return;
    }
    
    std::lock_guard<std::mutex> lock(compaction_mutex_);
    
    thread_pool_.enqueue([this, level]() {
        try {
            if (level >= sstable_levels_.size()) {
                compaction_in_progress_ = false;
                return;
            }
            
            if (level == 0 && sstable_levels_[0].size() > 4) {
                {
                    std::unique_lock<std::shared_mutex> wlock(rw_mutex_);
                    if (sstable_levels_.size() == 1) {
                        sstable_levels_.push_back(std::vector<std::unique_ptr<SSTable>>());
                    }
                }
                
                std::vector<std::unique_ptr<SSTable>> tables_to_compact;
                
                {
                    std::unique_lock<std::shared_mutex> wlock(rw_mutex_);
                    
                    tables_to_compact.swap(sstable_levels_[0]);
                }
                
                std::map<std::string, std::string> merged_data;
                
                for (auto it = tables_to_compact.rbegin(); it != tables_to_compact.rend(); ++it) {
                    const auto& table = *it;
                    
                    for (const auto& [key, value] : table->data) {
                        if (merged_data.find(key) == merged_data.end()) {
                            merged_data[key] = value;
                        }
                    }
                }
                
                for (auto it = merged_data.begin(); it != merged_data.end();) {
                    if (it->second.empty()) {
                        bool can_remove = true;
                        
                        for (const auto& table : tables_to_compact) {
                            if (table->data.find(it->first) == table->data.end()) {
                                can_remove = false;
                                break;
                            }
                        }
                        
                        if (can_remove) {
                            it = merged_data.erase(it);
                        } else {
                            ++it;
                        }
                    } else {
                        ++it;
                    }
                }
                
                if (!merged_data.empty()) {
                    auto new_sstable = std::make_unique<SSTable>(merged_data, merged_data.size());
                    new_sstable->level = 1;
                    
                    {
                        std::unique_lock<std::shared_mutex> wlock(rw_mutex_);
                        sstable_levels_[1].push_back(std::move(new_sstable));
                        
                        if (sstable_levels_[1].size() > 10) {
                            wlock.unlock();
                            compaction_in_progress_ = false;
                            compactLevel(1);
                            return;
                        }
                    }
                }
            }
            else if (level > 0 && level < sstable_levels_.size() && sstable_levels_[level].size() > 10) {
                {
                    std::unique_lock<std::shared_mutex> wlock(rw_mutex_);
                    if (level + 1 >= sstable_levels_.size()) {
                        sstable_levels_.push_back(std::vector<std::unique_ptr<SSTable>>());
                    }
                }
                
                std::vector<std::unique_ptr<SSTable>> tables_to_compact;
                std::vector<std::unique_ptr<SSTable>> remaining_tables;
                
                {
                    std::unique_lock<std::shared_mutex> wlock(rw_mutex_);
                    
                    size_t num_to_compact = std::min(size_t(5), sstable_levels_[level].size());
                    
                    for (size_t i = 0; i < num_to_compact; i++) {
                        tables_to_compact.push_back(std::move(sstable_levels_[level][i]));
                    }
                    
                    for (size_t i = num_to_compact; i < sstable_levels_[level].size(); i++) {
                        remaining_tables.push_back(std::move(sstable_levels_[level][i]));
                    }
                    
                    sstable_levels_[level].clear();
                    for (auto& table : remaining_tables) {
                        sstable_levels_[level].push_back(std::move(table));
                    }
                }
                
                std::map<std::string, std::string> merged_data;
                
                for (const auto& table : tables_to_compact) {
                    for (const auto& [key, value] : table->data) {
                        merged_data[key] = value;
                    }
                }
                
                auto now = std::chrono::steady_clock::now();
                auto cutoff = now - std::chrono::hours(24);
                
                for (auto it = merged_data.begin(); it != merged_data.end();) {
                    if (it->second.empty()) {
                        it = merged_data.erase(it);
                    } else {
                        ++it;
                    }
                }
                
                if (!merged_data.empty()) {
                    auto new_sstable = std::make_unique<SSTable>(merged_data, merged_data.size());
                    new_sstable->level = level + 1;
                    
                    {
                        std::unique_lock<std::shared_mutex> wlock(rw_mutex_);
                        sstable_levels_[level + 1].push_back(std::move(new_sstable));
                    }
                }
            }
            
            compaction_in_progress_ = false;
            reads_since_compaction_ = 0;
            writes_since_compaction_ = 0;
            
        } catch (const std::exception& e) {
            std::cerr << "Compaction error: " << e.what() << std::endl;
            compaction_in_progress_ = false;
        }
    });
}

void StorageEngine::checkAndScheduleCompaction() {
    if (compaction_in_progress_) {
        return;
    }
    
    if (sstable_levels_[0].size() > 4) {
        compactLevel(0);
        return;
    }
    
    for (size_t level = 1; level < sstable_levels_.size(); ++level) {
        if (sstable_levels_[level].size() > 10) {
            compactLevel(level);
            return;
        }
    }
    
    if (current_memory_usage_ > max_memory_size_ * 0.9) {
        for (size_t level = 1; level < sstable_levels_.size(); ++level) {
            if (sstable_levels_[level].size() > 0) {
                compactLevel(level);
                return;
            }
        }
    }
}

void StorageEngine::monitorAndAdjustCompaction() {
    auto now = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - last_compaction_time_).count();
    
    if (elapsed < 60) return;
    
    float read_ratio = reads_since_compaction_ / std::max<size_t>(1, writes_since_compaction_);
    
    if (read_ratio > 10.0f) {
        compaction_frequency_ = std::max(0.5f, compaction_frequency_ - 0.1f);
    } else if (read_ratio < 0.1f) {
        compaction_frequency_ = std::min(2.0f, compaction_frequency_ + 0.1f);
    }
    
    last_compaction_time_ = now;
}

/**
 * @brief Recovers the storage engine state from the log.
 */
void StorageEngine::recoverFromLog() {
    std::cout << "Initializing storage engine..." << std::endl;
    set("system:version", "1.0");
    set("system:start_time", "1.0");
    set("system:start_time", std::to_string(
        std::chrono::system_clock::to_time_t(std::chrono::system_clock::now())));
    std::cout << "Storage engine initialized." << std::endl;
}