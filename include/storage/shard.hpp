#pragma once

#include <string>
#include <unordered_map>
#include <vector>
#include <cstdint>
#include <cstdlib>
#include "common/concurrency.hpp"
#include "common/types.hpp"
#include "common/time_utils.hpp"

namespace flashkv {

class DbShard {
public:
    DbShard() = default;

    SharedMutex& mutex() {
        return mutex_;
    }

    // Direct access (caller MUST hold appropriate lock on mutex())
    ValueObject* find_unlocked(const std::string& key, int64_t now_ms) {
        auto it = entries_.find(key);
        if (it == entries_.end()) {
            return nullptr;
        }
        if (it->second.is_expired(now_ms)) {
            entries_.erase(it);
            return nullptr;
        }
        return &it->second;
    }

    void set_unlocked(const std::string& key, ValueObject&& val) {
        entries_[key] = std::move(val);
    }

    bool del_unlocked(const std::string& key) {
        return entries_.erase(key) > 0;
    }

    size_t size_unlocked() const {
        return entries_.size();
    }

    void clear_unlocked() {
        entries_.clear();
    }

    const std::unordered_map<std::string, ValueObject>& entries_unlocked() const {
        return entries_;
    }

    // Active eviction sample: checks up to 'sample_size' keys and erases expired ones
    size_t purge_expired_sample(size_t sample_size, int64_t now_ms) {
        UniqueLock lock(mutex_);
        if (entries_.empty()) return 0;

        size_t checked = 0;
        size_t purged = 0;
        auto it = entries_.begin();

        while (it != entries_.end() && checked < sample_size) {
            checked++;
            if (it->second.is_expired(now_ms)) {
                it = entries_.erase(it);
                purged++;
            } else {
                ++it;
            }
        }
        return purged;
    }

private:
    SharedMutex mutex_;
    std::unordered_map<std::string, ValueObject> entries_;

    DbShard(const DbShard&) = delete;
    DbShard& operator=(const DbShard&) = delete;
};

} // namespace flashkv
