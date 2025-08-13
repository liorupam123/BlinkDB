#ifndef STORAGE_ENGINE_H
#define STORAGE_ENGINE_H

#include <bits/stdc++.h>
#include "bloom.h"
using namespace std;

// Enum for WAL operation types
enum class WALEntryType : uint8_t {
    SET_OP = 0x01,
    DEL_OP = 0x02,
};

class StorageEngine {
private:
    struct MemTable;
    struct SSTable;
    
    struct KeyValue {
        string key;
        string value;
        uint64_t timestamp;
        bool is_deleted;

        KeyValue() : timestamp(0), is_deleted(false) {}
        KeyValue(const string& k, const string& v, uint64_t ts = 0, bool deleted = false) 
            : key(k), value(v), timestamp(ts), is_deleted(deleted) {}
    };

    struct MemTable {
        map<string, KeyValue> entries;
        size_t size_bytes;
        
        MemTable() : size_bytes(0) {}
        
        void put(const KeyValue& kv) {
            auto it = entries.find(kv.key);
            if (it != entries.end()) {
                size_bytes -= it->second.key.size() + it->second.value.size();
            }
            size_bytes += kv.key.size() + kv.value.size();
            entries[kv.key] = kv;
        }
        
        const KeyValue* get(const string& key) const {
            auto it = entries.find(key);
            return (it != entries.end()) ? &it->second : nullptr;
        }
        
        size_t size() const { return entries.size(); }
        bool empty() const { return entries.empty(); }
    };

    struct SSTable {
        string file_path;
        size_t level;
        map<string, size_t> index;
        string min_key;
        string max_key;
        BloomFilter* bloom_filter;

        SSTable(const string& path, size_t lvl) 
            : file_path(path), level(lvl), bloom_filter(nullptr) {}

        ~SSTable() {
            delete bloom_filter;
        }
    };

    struct CacheEntry {
        string key;
        string value;
        CacheEntry(const string& k, const string& v) : key(k), value(v) {}
    };

    MemTable* active_memtable;
    MemTable* immutable_memtable;
    vector<vector<SSTable*>> levels;
    size_t level_count;
    atomic<uint64_t> next_timestamp; // FIX: Ensure this is atomic
    atomic<bool> compaction_running;
    string db_directory;
    
    static const size_t CACHE_SIZE = 1024;
    list<CacheEntry> cache_list;
    unordered_map<string, list<CacheEntry>::iterator> cache_map;

    static const size_t MEMTABLE_MAX_SIZE = 4 * 1024 * 1024;
    static const size_t LEVEL_SIZE_RATIO = 10;
    
    mutable vector<mutex> level_mutexes;
    mutable mutex memtable_mutex;
    mutable mutex cache_mutex;
    thread compaction_thread;

    ofstream wal_file;
    string wal_path;
    mutable mutex wal_mutex;

    // FIX: Update signature to return optional<string>
    optional<string> get_from_cache(const string& key);
    void update_cache(const string& key, const string& value);

    void flush_memtable();
    void compact_level(size_t level);
    void merge_sstables(const vector<SSTable*>& input_tables, size_t next_level, vector<SSTable*>& output_tables);
    SSTable* create_sstable_from_memtable(MemTable* memtable, size_t level);
    unique_ptr<KeyValue> get_from_sstable(const SSTable* sstable, const string& key);
    void load_sstables();
    void start_background_compaction();
    void compaction_worker();
    uint64_t get_timestamp();
    
    void init_wal();
    void replay_wal();
    void log_wal_entry(WALEntryType type, const string& key, const string& value);
    void rotate_wal();

    // FIX: Unify signatures to take a single SSTable pointer
    void write_sstable_index(const SSTable* table);
    bool read_sstable_index(SSTable* table);
    string get_sstable_path(size_t level, uint64_t id);

public:
    StorageEngine(const string& db_dir = "blinkdb_data");
    ~StorageEngine();

    bool set(const char* key, const char* value);
    optional<string> get(const char* key);
    bool del(const char* key);
    void sync();
    // FIX: Ensure const matches implementation
    void debug_print_tree() const;
};

#endif