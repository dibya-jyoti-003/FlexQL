#pragma once
#include <unordered_map>
#include <map>
#include <vector>
#include <string>
#include <cstddef>

namespace flexql {

/*
 * PrimaryIndex
 * ------------
 * Hash-map index from primary-key value (string) → row index in the
 * table's row vector.
 *
 * Design choice:
 *   std::unordered_map gives O(1) average insert and lookup.
 *   For 10M rows this outperforms a B-tree for pure point lookups.
 *   The trade-off is no range-scan support, which is fine since the
 *   spec only requires equality WHERE conditions.
 */
class PrimaryIndex {
public:
    /* Record that row at position `row_idx` has primary key `key`. */
    void insert(const std::string &key, std::size_t row_idx);

    /*
     * Look up a primary key.
     * Returns the row index, or SIZE_MAX if not found.
     */
    std::size_t find(const std::string &key) const;

    /* Remove an entry (used if we ever support DELETE). */
    void remove(const std::string &key);

    /* Number of entries in the index. */
    std::size_t size() const;

    /* Pre-reserve capacity to avoid rehashing during bulk inserts. */
    void reserve(std::size_t capacity);

    void clear();

private:
    std::unordered_map<std::string, std::size_t> map_;
};

/*
 * SecondaryIndex
 * --------------
 * Tree-based index for secondary columns. Supports range scans.
 */
class SecondaryIndex {
public:
    void insert(const std::string &key, std::size_t row_idx, bool is_numeric = false);
    void find_range(const std::string &op, const std::string &value, std::vector<std::size_t> &out_indices, bool is_numeric = false) const;
    void clear();

private:
    std::multimap<std::string, std::size_t> string_map_;
    std::multimap<double, std::size_t>      numeric_map_;
};

} /* namespace flexql */
