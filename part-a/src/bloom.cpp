// bloom.cpp
#include "bloom.h"

BloomFilter::BloomFilter(uint64_t size, uint8_t num_hashes)
    : num_hashes_(num_hashes), bits_(size, false) {}

BloomFilter::BloomFilter(const std::vector<bool>& bits, uint8_t num_hashes)
    : num_hashes_(num_hashes), bits_(bits) {}

uint64_t BloomFilter::hash_func(const std::string& key, uint8_t seed) const {
    // Simple combination of std::hash and a seed
    return std::hash<std::string>{}(key) ^ (seed * 0x9e3779b9);
}

void BloomFilter::add(const std::string& key) {
    for (uint8_t i = 0; i < num_hashes_; ++i) {
        bits_[hash_func(key, i) % bits_.size()] = true;
    }
}

bool BloomFilter::possibly_contains(const std::string& key) const {
    for (uint8_t i = 0; i < num_hashes_; ++i) {
        if (!bits_[hash_func(key, i) % bits_.size()]) {
            return false;
        }
    }
    return true;
}

const std::vector<bool>& BloomFilter::get_bits() const {
    return bits_;
}

uint8_t BloomFilter::get_num_hashes() const {
    return num_hashes_;
}