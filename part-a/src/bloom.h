// bloom.h
#ifndef BLOOM_H
#define BLOOM_H

#include <vector>
#include <string>
#include <cstdint>
#include <functional>

/**
 * @class BloomFilter
 * @brief A simple Bloom Filter implementation for probabilistic key existence checks.
 */
class BloomFilter {
public:
    /**
     * @brief Constructs a BloomFilter with a given size and number of hash functions.
     * @param size The number of bits in the filter.
     * @param num_hashes The number of hash functions to use.
     */
    BloomFilter(uint64_t size, uint8_t num_hashes);

    /**
     * @brief Constructs a BloomFilter from an existing bit vector.
     * @param bits The bit vector to load.
     * @param num_hashes The number of hash functions used to create the filter.
     */
    BloomFilter(const std::vector<bool>& bits, uint8_t num_hashes);

    /**
     * @brief Adds a key to the filter.
     * @param key The key to add.
     */
    void add(const std::string& key);

    /**
     * @brief Checks if a key is possibly in the set.
     * @param key The key to check.
     * @return False if the key is definitely not in the set, true if it might be.
     */
    bool possibly_contains(const std::string& key) const;

    /**
     * @brief Gets the bit array of the filter for serialization.
     * @return A constant reference to the bit vector.
     */
    const std::vector<bool>& get_bits() const;
    
    /**
     * @brief Gets the number of hash functions used.
     * @return The number of hashes.
     */
    uint8_t get_num_hashes() const;

private:
    // Simple seeded hash function
    uint64_t hash_func(const std::string& key, uint8_t seed) const;

    uint8_t num_hashes_;
    std::vector<bool> bits_;
};

#endif // BLOOM_H