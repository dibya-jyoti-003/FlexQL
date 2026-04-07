#include "../include/cache.h"

namespace flexql {

LRUCache::LRUCache(std::size_t capacity) : capacity_(capacity) {}

void LRUCache::put(const std::string &sql, const ResultSet &result) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = index_.find(sql);
    if (it != index_.end()) {
        /* already cached – move to front and update */
        lru_list_.erase(it->second);
        index_.erase(it);
    }
    if (lru_list_.size() >= capacity_) evict_lru();

    lru_list_.push_front({sql, result});
    index_[sql] = lru_list_.begin();
}

bool LRUCache::get(const std::string &sql, ResultSet &out) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = index_.find(sql);
    if (it == index_.end()) return false;

    /* promote to front */
    lru_list_.splice(lru_list_.begin(), lru_list_, it->second);
    out = it->second->second;
    return true;
}

void LRUCache::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    lru_list_.clear();
    index_.clear();
}

void LRUCache::invalidate_table(const std::string &table_name) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto it = lru_list_.begin(); it != lru_list_.end(); ) {
        const std::string &key = it->first;
        bool match = false;
        
        /* check if the key contains the table name as a component (delimited by ::) */
        if (key.find(table_name + "::") == 0) {
            match = true;
        } else if (key.find("::" + table_name + "::") != std::string::npos) {
            match = true;
        } else if (key.size() >= table_name.size() && key.substr(key.size() - table_name.size()) == table_name && key.find("::" + table_name) != std::string::npos) {
             /* cases like "...::JOIN::table_b" where table_b is the table to invalidate */
             match = true;
        }

        if (match) {
            index_.erase(key);
            it = lru_list_.erase(it);
        } else {
            ++it;
        }
    }
}

std::size_t LRUCache::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return lru_list_.size();
}

std::size_t LRUCache::capacity() const {
    return capacity_;
}

void LRUCache::evict_lru() {
    if (lru_list_.empty()) return;
    auto last = std::prev(lru_list_.end());
    index_.erase(last->first);
    lru_list_.erase(last);
}

} /* namespace flexql */
