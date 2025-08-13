#include "storage_engine.h"
#include <filesystem>
#include <sys/stat.h>
#include <algorithm>
#include <fstream>

namespace fs = std::filesystem;

// --- Constructor & Destructor ---

StorageEngine::StorageEngine(const string& db_dir) 
    : active_memtable(new MemTable()),
      immutable_memtable(nullptr),
      level_count(7),
      next_timestamp(1),
      compaction_running(false),
      db_directory(db_dir),
      level_mutexes(level_count) {
    
    if (!fs::exists(db_directory)) {
        fs::create_directories(db_directory);
    }
    
    levels.resize(level_count);

    init_wal();
    replay_wal();

    load_sstables();
    start_background_compaction();
    cout << "BlinkDB Engine Initialized: WAL, Bloom Filters, and Multi-Threaded Compaction are active." << endl;
}

StorageEngine::~StorageEngine() {
    compaction_running = false;
    if (compaction_thread.joinable()) {
        compaction_thread.join();
    }
    sync();
    
    delete active_memtable;
    delete immutable_memtable;
    
    for (auto& level : levels) {
        for (auto* table : level) {
            delete table;
        }
    }
    if (wal_file.is_open()) wal_file.close();
}


// --- Public API ---
bool StorageEngine::set(const char* key, const char* value) {
    if (!key || !value) return false;
    string key_str(key), value_str(value);
    log_wal_entry(WALEntryType::SET_OP, key_str, value_str);
    update_cache(key_str, value_str);
    lock_guard<mutex> lock(memtable_mutex);
    active_memtable->put(KeyValue(key_str, value_str, get_timestamp(), false));
    if (active_memtable->size_bytes >= MEMTABLE_MAX_SIZE) {
        if (immutable_memtable) {
            flush_memtable();
        }
        immutable_memtable = active_memtable;
        active_memtable = new MemTable();
        thread(&StorageEngine::flush_memtable, this).detach();
    }
    return true;
}

optional<string> StorageEngine::get(const char* key) {
    if (!key) return nullopt;
    string key_str(key);
    if (auto val = get_from_cache(key_str)) {
        return val;
    }
    {
        lock_guard<mutex> lock(memtable_mutex);
        if (auto* kv = active_memtable->get(key_str)) {
            if (kv->is_deleted) return nullopt;
            update_cache(key_str, kv->value);
            return kv->value;
        }
        if (immutable_memtable) {
             if (auto* kv = immutable_memtable->get(key_str)) {
                if (kv->is_deleted) return nullopt;
                update_cache(key_str, kv->value);
                return kv->value;
            }
        }
    }
    unique_ptr<KeyValue> latest_kv = nullptr;
    for (size_t i = 0; i < levels.size(); ++i) {
        lock_guard<mutex> level_lock(level_mutexes[i]);
        for (auto it = levels[i].rbegin(); it != levels[i].rend(); ++it) {
            if ((*it)->min_key <= key_str && (*it)->max_key >= key_str) {
                 unique_ptr<KeyValue> kv = get_from_sstable(*it, key_str);
                 if (kv && (!latest_kv || kv->timestamp > latest_kv->timestamp)) {
                    latest_kv = move(kv);
                }
            }
        }
    }
    if (latest_kv && !latest_kv->is_deleted) {
        update_cache(key_str, latest_kv->value);
        return latest_kv->value;
    }
    return nullopt;
}

bool StorageEngine::del(const char* key) {
    if (!key) return false;
    string key_str(key);
    log_wal_entry(WALEntryType::DEL_OP, key_str, "");
    {
        lock_guard<mutex> lock(cache_mutex);
        if (auto it = cache_map.find(key_str); it != cache_map.end()) {
            cache_list.erase(it->second);
            cache_map.erase(it);
        }
    }
    lock_guard<mutex> lock(memtable_mutex);
    active_memtable->put(KeyValue(key_str, "", get_timestamp(), true));
    if (active_memtable->size_bytes >= MEMTABLE_MAX_SIZE) {
        if (immutable_memtable) {
            flush_memtable();
        }
        immutable_memtable = active_memtable;
        active_memtable = new MemTable();
        thread(&StorageEngine::flush_memtable, this).detach();
    }
    return true;
}

void StorageEngine::sync() {
    lock_guard<mutex> lock(memtable_mutex);
    if (active_memtable && !active_memtable->empty()) {
        if (immutable_memtable) {
            flush_memtable();
        }
        immutable_memtable = active_memtable;
        active_memtable = new MemTable();
    }
    flush_memtable();
}

void StorageEngine::debug_print_tree() const {
    lock_guard<mutex> mem_lock(memtable_mutex);
    
    cout << "=== LSM Tree Structure ===" << endl;
    cout << "Active memtable: " << active_memtable->size() << " keys, " << active_memtable->size_bytes << " bytes" << endl;
    if (immutable_memtable) {
        cout << "Immutable memtable: " << immutable_memtable->size() << " keys, " << immutable_memtable->size_bytes << " bytes" << endl;
    } else {
        cout << "Immutable memtable: none" << endl;
    }

    for (size_t i = 0; i < levels.size(); ++i) {
        lock_guard<mutex> level_lock(level_mutexes[i]);
        
        cout << "Level " << i << ": " << levels[i].size() << " SSTables" << endl;
        for (const auto* table : levels[i]) {
            cout << "  - Table: " << fs::path(table->file_path).filename().string() 
                 << ", Keys: " << table->index.size()
                 << ", Range: [" << table->min_key << " - " << table->max_key << "]" << endl;
        }
    }
}


// --- Cache Operations ---
void StorageEngine::update_cache(const string& key, const string& value) {
    lock_guard<mutex> lock(cache_mutex);
    auto it = cache_map.find(key);
    if (it != cache_map.end()) {
        it->second->value = value;
        cache_list.splice(cache_list.begin(), cache_list, it->second);
    } else {
        if (cache_list.size() >= CACHE_SIZE) {
            cache_map.erase(cache_list.back().key);
            cache_list.pop_back();
        }
        cache_list.emplace_front(key, value);
        cache_map[key] = cache_list.begin();
    }
}

optional<string> StorageEngine::get_from_cache(const string& key) {
    lock_guard<mutex> lock(cache_mutex);
    auto it = cache_map.find(key);
    if (it == cache_map.end()) {
        return nullopt;
    }
    cache_list.splice(cache_list.begin(), cache_list, it->second);
    return it->second->value;
}


// --- WAL (Write-Ahead Log) Implementation ---
void StorageEngine::init_wal() {
    wal_path = db_directory + "/wal.log";
}

void StorageEngine::replay_wal() {
    ifstream wal_in(wal_path, ios::binary);
    if (!wal_in) return;
    cout << "Replaying Write-Ahead Log..." << endl;
    char type_char;
    while (wal_in.read(&type_char, sizeof(char))) {
        WALEntryType type = static_cast<WALEntryType>(type_char);
        uint32_t key_size, value_size = 0;
        wal_in.read(reinterpret_cast<char*>(&key_size), sizeof(key_size));
        string key(key_size, '\0');
        wal_in.read(&key[0], key_size);
        string value;
        if (type == WALEntryType::SET_OP) {
            wal_in.read(reinterpret_cast<char*>(&value_size), sizeof(value_size));
            value.resize(value_size);
            wal_in.read(&value[0], value_size);
        }
        active_memtable->put(KeyValue(key, value, get_timestamp(), type == WALEntryType::DEL_OP));
    }
    cout << "WAL replay finished. " << active_memtable->size() << " operations recovered." << endl;
}

void StorageEngine::log_wal_entry(WALEntryType type, const string& key, const string& value) {
    lock_guard<mutex> lock(wal_mutex);
    if (!wal_file.is_open()) {
        wal_file.open(wal_path, ios::binary | ios::app);
    }
    wal_file.write(reinterpret_cast<const char*>(&type), sizeof(uint8_t));
    uint32_t key_size = key.size();
    wal_file.write(reinterpret_cast<const char*>(&key_size), sizeof(key_size));
    wal_file.write(key.c_str(), key_size);
    if (type == WALEntryType::SET_OP) {
        uint32_t value_size = value.size();
        wal_file.write(reinterpret_cast<const char*>(&value_size), sizeof(value_size));
        wal_file.write(value.c_str(), value_size);
    }
    wal_file.flush();
}

void StorageEngine::rotate_wal() {
    lock_guard<mutex> lock(wal_mutex);
    if (wal_file.is_open()) wal_file.close();
    fs::remove(wal_path);
    wal_file.open(wal_path, ios::binary | ios::trunc); 
    lock_guard<mutex> mem_lock(memtable_mutex);
    if (active_memtable) {
        for (const auto& [key, kv] : active_memtable->entries) {
            WALEntryType type = kv.is_deleted ? WALEntryType::DEL_OP : WALEntryType::SET_OP;
            log_wal_entry(type, kv.key, kv.value);
        }
    }
}


// --- Compaction & SSTable Management ---
void StorageEngine::flush_memtable() {
    MemTable* mem_to_flush = nullptr;
    {
        lock_guard<mutex> lock(memtable_mutex);
        if (!immutable_memtable) return;
        mem_to_flush = immutable_memtable;
        immutable_memtable = nullptr;
    }
    if (mem_to_flush && !mem_to_flush->empty()) {
        SSTable* new_table = create_sstable_from_memtable(mem_to_flush, 0);
        if (new_table) {
            lock_guard<mutex> lock(level_mutexes[0]);
            levels[0].push_back(new_table);
        }
    }
    delete mem_to_flush;
    rotate_wal();
}

void StorageEngine::compact_level(size_t level) {
    if (level >= levels.size() - 1) return;
    unique_lock<mutex> lock_level(level_mutexes[level], defer_lock);
    unique_lock<mutex> lock_next_level(level_mutexes[level + 1], defer_lock);
    lock(lock_level, lock_next_level);
    if (levels[level].empty()) return;
    vector<SSTable*> tables_to_compact = levels[level];
    levels[level].clear();
    string min_key = tables_to_compact.front()->min_key;
    string max_key = tables_to_compact.back()->max_key;
    vector<SSTable*> next_level_overlap;
    vector<SSTable*> next_level_remaining;
    for (SSTable* table : levels[level+1]) {
        if (max(min_key, table->min_key) <= min(max_key, table->max_key)) {
            next_level_overlap.push_back(table);
        } else {
            next_level_remaining.push_back(table);
        }
    }
    levels[level+1] = next_level_remaining;
    tables_to_compact.insert(tables_to_compact.end(), next_level_overlap.begin(), next_level_overlap.end());
    vector<SSTable*> output_tables;
    merge_sstables(tables_to_compact, level + 1, output_tables);
    levels[level+1].insert(levels[level+1].end(), output_tables.begin(), output_tables.end());
    sort(levels[level+1].begin(), levels[level+1].end(), 
        [](auto a, auto b){ return a->min_key < b->min_key; });
    lock_level.unlock();
    lock_next_level.unlock();
    for(auto* table : tables_to_compact) {
        fs::remove(table->file_path);
        fs::remove(table->file_path + ".index");
        delete table;
    }
}

void StorageEngine::merge_sstables(const vector<SSTable*>& input_tables, size_t next_level, vector<SSTable*>& output_tables) {
    map<string, KeyValue> merged_data;
    for (SSTable* table : input_tables) {
        ifstream file(table->file_path, ios::binary);
        for (const auto& [key, offset] : table->index) {
            file.seekg(offset);
            uint32_t key_size, value_size;
            uint64_t timestamp;
            bool is_deleted;
            file.read(reinterpret_cast<char*>(&key_size), sizeof(key_size));
            string current_key(key_size, '\0');
            file.read(&current_key[0], key_size);
            file.read(reinterpret_cast<char*>(&value_size), sizeof(value_size));
            string value(value_size, '\0');
            file.read(&value[0], value_size);
            file.read(reinterpret_cast<char*>(&timestamp), sizeof(timestamp));
            file.read(reinterpret_cast<char*>(&is_deleted), sizeof(is_deleted));
            if (merged_data.find(key) == merged_data.end() || timestamp > merged_data[key].timestamp) {
                merged_data[key] = KeyValue(key, value, timestamp, is_deleted);
            }
        }
    }
    if (merged_data.empty()) return;
    SSTable* output_table = new SSTable(get_sstable_path(next_level, get_timestamp()), next_level);
    ofstream out_file(output_table->file_path, ios::binary);
    output_table->min_key = merged_data.begin()->first;
    output_table->max_key = merged_data.rbegin()->first;
    uint64_t bloom_size = merged_data.size() * 10;
    uint8_t bloom_hashes = 7;
    output_table->bloom_filter = new BloomFilter(bloom_size, bloom_hashes);
    for (const auto& [key, kv] : merged_data) {
        if (kv.is_deleted && next_level > 0) continue;
        output_table->bloom_filter->add(key);
        size_t pos = out_file.tellp();
        output_table->index[key] = pos;
        uint32_t key_size = kv.key.size();
        out_file.write(reinterpret_cast<const char*>(&key_size), sizeof(key_size));
        out_file.write(kv.key.c_str(), key_size);
        uint32_t value_size = kv.value.size();
        out_file.write(reinterpret_cast<const char*>(&value_size), sizeof(value_size));
        out_file.write(kv.value.c_str(), value_size);
        out_file.write(reinterpret_cast<const char*>(&kv.timestamp), sizeof(kv.timestamp));
        out_file.write(reinterpret_cast<const char*>(&kv.is_deleted), sizeof(kv.is_deleted));
    }
    out_file.close();
    write_sstable_index(output_table);
    output_tables.push_back(output_table);
}

StorageEngine::SSTable* StorageEngine::create_sstable_from_memtable(MemTable* memtable, size_t level) {
    if (memtable->entries.empty()) return nullptr;
    auto* table = new SSTable(get_sstable_path(level, get_timestamp()), level);
    uint64_t bloom_size = memtable->size() * 10;
    uint8_t bloom_hashes = 7;
    table->bloom_filter = new BloomFilter(bloom_size, bloom_hashes);
    ofstream file(table->file_path, ios::binary);
    if (!file) { delete table; return nullptr; }
    table->min_key = memtable->entries.begin()->second.key;
    table->max_key = memtable->entries.rbegin()->second.key;
    for (const auto& [key, kv] : memtable->entries) {
        table->bloom_filter->add(key);
        size_t pos = file.tellp();
        table->index[key] = pos;
        uint32_t key_size = kv.key.size();
        file.write(reinterpret_cast<const char*>(&key_size), sizeof(key_size));
        file.write(kv.key.c_str(), key_size);
        uint32_t value_size = kv.value.size();
        file.write(reinterpret_cast<const char*>(&value_size), sizeof(value_size));
        file.write(kv.value.c_str(), value_size);
        file.write(reinterpret_cast<const char*>(&kv.timestamp), sizeof(kv.timestamp));
        file.write(reinterpret_cast<const char*>(&kv.is_deleted), sizeof(kv.is_deleted));
    }
    file.close();
    write_sstable_index(table);
    return table;
}

// THIS IS THE CORRECTED FUNCTION
unique_ptr<StorageEngine::KeyValue> StorageEngine::get_from_sstable(const SSTable* sstable, const string& key) {
    // FIX: Check if the bloom_filter pointer is valid before using it.
    if (sstable->bloom_filter && !sstable->bloom_filter->possibly_contains(key)) {
        return nullptr;
    }

    auto it = sstable->index.find(key);
    if (it == sstable->index.end()) return nullptr;
    
    ifstream file(sstable->file_path, ios::binary);
    if (!file) return nullptr;
    
    file.seekg(it->second);
    
    auto result = make_unique<KeyValue>();
    result->key = key;

    uint32_t key_size, value_size;
    
    file.read(reinterpret_cast<char*>(&key_size), sizeof(key_size));
    string stored_key(key_size, '\0');
    file.read(&stored_key[0], key_size);
    if (stored_key != key) return nullptr;
    
    file.read(reinterpret_cast<char*>(&value_size), sizeof(value_size));
    result->value.resize(value_size);
    file.read(&result->value[0], value_size);
    
    file.read(reinterpret_cast<char*>(&result->timestamp), sizeof(result->timestamp));
    file.read(reinterpret_cast<char*>(&result->is_deleted), sizeof(result->is_deleted));
    
    return result;
}


// --- File & System Operations ---
void StorageEngine::load_sstables() {
    for (size_t i = 0; i < level_count; ++i) {
        string level_dir = db_directory + "/L" + to_string(i);
        if (!fs::exists(level_dir)) continue;
        for (const auto& entry : fs::directory_iterator(level_dir)) {
            if (entry.path().extension() == ".sst") {
                SSTable* table = new SSTable(entry.path().string(), i);
                if (read_sstable_index(table)) {
                    levels[i].push_back(table);
                } else {
                    delete table;
                }
            }
        }
        sort(levels[i].begin(), levels[i].end(), [](auto a, auto b) { return a->min_key < b->min_key; });
    }
}

void StorageEngine::write_sstable_index(const SSTable* table) {
    ofstream file(table->file_path + ".index", ios::binary);
    if (!file) return;
    size_t num_entries = table->index.size();
    file.write(reinterpret_cast<const char*>(&num_entries), sizeof(num_entries));
    uint32_t min_key_size = table->min_key.size();
    file.write(reinterpret_cast<const char*>(&min_key_size), sizeof(min_key_size));
    file.write(table->min_key.c_str(), min_key_size);
    uint32_t max_key_size = table->max_key.size();
    file.write(reinterpret_cast<const char*>(&max_key_size), sizeof(max_key_size));
    file.write(table->max_key.c_str(), max_key_size);
    if (table->bloom_filter) {
        const auto& bits = table->bloom_filter->get_bits();
        uint64_t bit_count = bits.size();
        uint8_t num_hashes = table->bloom_filter->get_num_hashes();
        file.write(reinterpret_cast<const char*>(&bit_count), sizeof(bit_count));
        file.write(reinterpret_cast<const char*>(&num_hashes), sizeof(num_hashes));
        for (bool bit : bits) {
            char b = bit;
            file.write(&b, sizeof(char));
        }
    }
    for (const auto& [key, offset] : table->index) {
        uint32_t key_size = key.size();
        file.write(reinterpret_cast<const char*>(&key_size), sizeof(key_size));
        file.write(key.c_str(), key_size);
        file.write(reinterpret_cast<const char*>(&offset), sizeof(offset));
    }
}

bool StorageEngine::read_sstable_index(SSTable* table) {
    ifstream file(table->file_path + ".index", ios::binary);
    if (!file) return false;
    size_t num_entries;
    file.read(reinterpret_cast<char*>(&num_entries), sizeof(num_entries));
    if (!file) return false;
    uint32_t min_key_size, max_key_size;
    file.read(reinterpret_cast<char*>(&min_key_size), sizeof(min_key_size));
    table->min_key.resize(min_key_size);
    file.read(&table->min_key[0], min_key_size);
    file.read(reinterpret_cast<char*>(&max_key_size), sizeof(max_key_size));
    table->max_key.resize(max_key_size);
    file.read(&table->max_key[0], max_key_size);
    uint64_t bit_count;
    if(file.read(reinterpret_cast<char*>(&bit_count), sizeof(bit_count))) {
        const uint64_t MAX_REASONABLE_BLOOM_BITS = 200 * 1024 * 1024 * 8; // 200 MB
        if (bit_count > 0 && bit_count < MAX_REASONABLE_BLOOM_BITS) {
            uint8_t num_hashes;
            file.read(reinterpret_cast<char*>(&num_hashes), sizeof(num_hashes));
            vector<bool> bits(bit_count);
            for(uint64_t i = 0; i < bit_count; ++i) {
                char b;
                if(!file.read(&b, sizeof(char))) return false;
                bits[i] = b;
            }
            table->bloom_filter = new BloomFilter(bits, num_hashes);
        } else {
            // This is not an error, just an old file. We don't load a filter.
        }
    }
    for(size_t i = 0; i < num_entries; ++i) {
        uint32_t key_size;
        file.read(reinterpret_cast<char*>(&key_size), sizeof(key_size));
        if (!file) return true;
        string key(key_size, '\0');
        file.read(&key[0], key_size);
        size_t offset;
        file.read(reinterpret_cast<char*>(&offset), sizeof(offset));
        if (!file) return false;
        table->index[key] = offset;
    }
    return true;
}

void StorageEngine::start_background_compaction() {
    compaction_thread = thread(&StorageEngine::compaction_worker, this);
}

void StorageEngine::compaction_worker() {
    compaction_running = true;
    while(compaction_running) {
        this_thread::sleep_for(chrono::seconds(2));
        for (size_t i = 0; i < levels.size() - 1; ++i) {
            if (level_mutexes[i].try_lock()) {
                size_t max_tables = (i == 0) ? 4 : LEVEL_SIZE_RATIO;
                if (levels[i].size() > max_tables) {
                    level_mutexes[i].unlock();
                    thread(&StorageEngine::compact_level, this, i).detach();
                } else {
                    level_mutexes[i].unlock();
                }
            }
        }
    }
}

uint64_t StorageEngine::get_timestamp() {
    return next_timestamp.fetch_add(1, memory_order_relaxed);
}

string StorageEngine::get_sstable_path(size_t level, uint64_t id) {
    string level_dir = db_directory + "/L" + to_string(level);
    fs::create_directories(level_dir);
    return level_dir + "/table_" + to_string(id) + ".sst";
}