#include "../include/index.h"
#include <climits>
#include <cstdint>
#include <cstddef>

namespace flexql {

void PrimaryIndex::insert(const std::string &key, std::size_t row_idx) {
    map_[key] = row_idx;
}

std::size_t PrimaryIndex::find(const std::string &key) const {
    auto it = map_.find(key);
    if (it == map_.end()) return SIZE_MAX;
    return it->second;
}

void PrimaryIndex::remove(const std::string &key) {
    map_.erase(key);
}

std::size_t PrimaryIndex::size() const {
    return map_.size();
}

void PrimaryIndex::reserve(std::size_t capacity) {
    map_.reserve(capacity);
}

void PrimaryIndex::clear() {
    map_.clear();
}

void SecondaryIndex::insert(const std::string &key, std::size_t row_idx, bool is_numeric) {
    if (is_numeric) {
        try {
            numeric_map_.emplace(std::stold(key), row_idx);
            return;
        } catch (...) { /* fallback */ }
    }
    string_map_.emplace(key, row_idx);
}

void SecondaryIndex::find_range(const std::string &op, const std::string &value, std::vector<std::size_t> &out_indices, bool is_numeric) const {
    if (is_numeric) {
        try {
            long double k = std::stold(value);
            if (op == "=") {
                auto r = numeric_map_.equal_range(k);
                for (auto it = r.first; it != r.second; ++it) out_indices.push_back(it->second);
            } else if (op == ">") {
                auto it = numeric_map_.upper_bound(k);
                for (; it != numeric_map_.end(); ++it) out_indices.push_back(it->second);
            } else if (op == ">=") {
                auto it = numeric_map_.lower_bound(k);
                for (; it != numeric_map_.end(); ++it) out_indices.push_back(it->second);
            } else if (op == "<") {
                auto it = numeric_map_.begin();
                auto end = numeric_map_.lower_bound(k);
                for (; it != end; ++it) out_indices.push_back(it->second);
            } else if (op == "<=") {
                auto it = numeric_map_.begin();
                auto end = numeric_map_.upper_bound(k);
                for (; it != end; ++it) out_indices.push_back(it->second);
            }
            return;
        } catch (...) { /* fallback to string map if parsing fails */ }
    }

    /* string-based lookup */
    if (op == "=") {
        auto r = string_map_.equal_range(value);
        for (auto it = r.first; it != r.second; ++it) out_indices.push_back(it->second);
    } else if (op == ">") {
        auto it = string_map_.upper_bound(value);
        for (; it != string_map_.end(); ++it) out_indices.push_back(it->second);
    } else if (op == ">=") {
        auto it = string_map_.lower_bound(value);
        for (; it != string_map_.end(); ++it) out_indices.push_back(it->second);
    } else if (op == "<") {
        auto it = string_map_.begin();
        auto end = string_map_.lower_bound(value);
        for (; it != end; ++it) out_indices.push_back(it->second);
    } else if (op == "<=") {
        auto it = string_map_.begin();
        auto end = string_map_.upper_bound(value);
        for (; it != end; ++it) out_indices.push_back(it->second);
    }
}

void SecondaryIndex::clear() {
    string_map_.clear();
    numeric_map_.clear();
}

} /* namespace flexql */
