#pragma once

#include <atomic>
#include <functional>
#include <limits>

#include "MurmurHash3.hpp"

template <typename T, T empty = std::numeric_limits<T>::max(), T tombstone = std::numeric_limits<T>::max() - 1>
class HashSet {
public:
    static const T empty_bucket = empty;
    static const T tombstone_bucket = tombstone;

    HashSet(size_t bucket_count) : num_buckets(bucket_count) {
        buckets = new std::atomic<T>[num_buckets];
        for (size_t i = 0; i < num_buckets; i++)
            *reinterpret_cast<T*>(buckets + i) = empty;
    }

    ~HashSet() {
        delete[] buckets;
    }

    bool insert(const T& key) {
        size_t hash = 0;
        MurmurHash3_x86_32(&key, sizeof(T), 1, &hash);
        size_t i = hash % num_buckets;
        // NOTE: for proper set semantics we would first have to check if key is already in the set and not insert in that case, but the use case here does not require checking this at this level, so we don't.
        while (true) {
            T bucket_val = buckets[i].load();
            if (bucket_val == empty || bucket_val == tombstone) {
                if (buckets[i].compare_exchange_strong(bucket_val, key))
                    return true;
            }
            i = (i + 1) % num_buckets;
        }
    }

    size_t erase(const T& key) {
        size_t hash = 0;
        MurmurHash3_x86_32(&key, sizeof(T), 1, &hash);
        size_t offset = 0;
        while (offset != num_buckets) {
            size_t i = (hash + offset) % num_buckets;
            T bucket_val = buckets[i].load();
            if (bucket_val == empty) {
                return 0;
            } else if (bucket_val == key) {
                if (buckets[i].compare_exchange_strong(bucket_val, tombstone))
                    return 1;
            }
            offset++;
        }
        return 0;
    }

    size_t bucketCount() const { return num_buckets; }

    inline T getBucket(size_t i) const { return buckets[i].load(); }

private:
    const size_t num_buckets;
    std::atomic<T>* buckets;
};