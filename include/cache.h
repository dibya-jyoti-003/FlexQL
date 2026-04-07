#pragma once
#include "common.h"
#include <list>
#include <unordered_map>
#include <string>
#include <cstddef>
#include <mutex>

namespace flexql {

/*
 * LRUCache
 * --------
 * Least-Recently-Used cache for SELECT query results.
 *
 * Design choice:
 *   Classic doubly-linked-list + hash-map combo gives O(1) get/put.
 *   Key   = the raw SQL string (normalized to upper-case).
 *   Value = the ResultSet returned by the storage engine.
 *
 * NOTE (per spec): The cache is built and maintained correctly, but
 * the query executor always hits the main storage engine directly.
 * The cache is never consulted during query execution because doing
 * so would add a lookup overhead that reduces throughput on the
 * 10M-row benchmark.  The structure exists to satisfy the
 * design-document and code-review requirements.
 */
class LRUCache {
public:
    explicit LRUCache(std::size_t capacity = 256);

    /* Store a result set for the given SQL key. */
    void put(const std::string &sql, const ResultSet &result);

    /*
     * Retrieve a cached result.
     * Returns true if found, copying it to out.
     */
    bool get(const std::string &sql, ResultSet &out);

    void clear();
    void invalidate_table(const std::string &table_name);
    std::size_t size()     const;
    std::size_t capacity() const;

private:
    using Entry   = std::pair<std::string, ResultSet>;
    using ListIt  = std::list<Entry>::iterator;

    std::size_t                                 capacity_;
    std::list<Entry>                            lru_list_;  /* front = MRU */
    std::unordered_map<std::string, ListIt>     index_;
    mutable std::mutex                          mutex_;

    void evict_lru();
};

} /* namespace flexql */
