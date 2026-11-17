#pragma once

#include <vector>
#include <string>
#include <memory>
#include <cstdint>
#include <atomic>
#include <algorithm>
#include "storage/shard.hpp"
#include "common/types.hpp"
#include "common/time_utils.hpp"
#include "protocol/resp.hpp"

namespace flashkv {

class StorageEngine {
public:
    static constexpr size_t DEFAULT_SHARDS = 64;

    explicit StorageEngine(size_t num_shards = DEFAULT_SHARDS)
        : num_shards_(num_shards), mask_(num_shards - 1),
          total_commands_(0), total_reads_(0), total_writes_(0) {
        // Ensure num_shards is power of two
        if ((num_shards & (num_shards - 1)) != 0) {
            num_shards_ = DEFAULT_SHARDS;
            mask_ = num_shards_ - 1;
        }
        shards_ = new DbShard[num_shards_];
    }

    ~StorageEngine() {
        delete[] shards_;
    }

    // FNV-1a 64-bit hash function
    static uint64_t hash_key(const std::string& key) {
        uint64_t hash = 14695981039346656037ULL;
        for (char c : key) {
            hash ^= static_cast<uint8_t>(c);
            hash *= 1099511628211ULL;
        }
        return hash;
    }

    DbShard& get_shard(const std::string& key) {
        return shards_[hash_key(key) & mask_];
    }

    size_t shard_count() const {
        return num_shards_;
    }

    DbShard& get_shard_by_index(size_t index) {
        return shards_[index];
    }

    // ==========================================
    // String Operations
    // ==========================================
    bool set(const std::string& key, const std::string& val, int64_t expire_at_ms = 0) {
        total_writes_.fetch_add(1, std::memory_order_relaxed);
        DbShard& shard = get_shard(key);
        UniqueLock lock(shard.mutex());
        shard.set_unlocked(key, ValueObject::make_string(val, expire_at_ms));
        return true;
    }

    bool get(const std::string& key, std::string& out_val) {
        total_reads_.fetch_add(1, std::memory_order_relaxed);
        DbShard& shard = get_shard(key);
        int64_t now = TimeUtils::now_ms();
        SharedLock lock(shard.mutex());
        ValueObject* obj = shard.find_unlocked(key, now);
        if (!obj || obj->type != DataType::STRING) {
            return false;
        }
        out_val = obj->string_val;
        return true;
    }

    bool del(const std::string& key) {
        total_writes_.fetch_add(1, std::memory_order_relaxed);
        DbShard& shard = get_shard(key);
        UniqueLock lock(shard.mutex());
        return shard.del_unlocked(key);
    }

    bool exists(const std::string& key) {
        total_reads_.fetch_add(1, std::memory_order_relaxed);
        DbShard& shard = get_shard(key);
        int64_t now = TimeUtils::now_ms();
        SharedLock lock(shard.mutex());
        return shard.find_unlocked(key, now) != nullptr;
    }

    DataType get_type(const std::string& key) {
        DbShard& shard = get_shard(key);
        int64_t now = TimeUtils::now_ms();
        SharedLock lock(shard.mutex());
        ValueObject* obj = shard.find_unlocked(key, now);
        return obj ? obj->type : DataType::NONE;
    }

    // INCRBY: increment integer string
    bool incr_by(const std::string& key, int64_t delta, int64_t& out_new_val, std::string& err_msg) {
        total_writes_.fetch_add(1, std::memory_order_relaxed);
        DbShard& shard = get_shard(key);
        int64_t now = TimeUtils::now_ms();
        UniqueLock lock(shard.mutex());
        ValueObject* obj = shard.find_unlocked(key, now);

        if (!obj) {
            // Create new string with value = delta
            out_new_val = delta;
            shard.set_unlocked(key, ValueObject::make_string(std::to_string(delta)));
            return true;
        }

        if (obj->type != DataType::STRING) {
            err_msg = "WRONGTYPE Operation against a key holding the wrong kind of value";
            return false;
        }

        char* endptr = nullptr;
        long long current = std::strtoll(obj->string_val.c_str(), &endptr, 10);
        if (*endptr != '\0') {
            err_msg = "ERR value is not an integer or out of range";
            return false;
        }

        current += delta;
        obj->string_val = std::to_string(current);
        out_new_val = current;
        return true;
    }

    // ==========================================
    // Hash Operations
    // ==========================================
    int hset(const std::string& key, const std::string& field, const std::string& val) {
        total_writes_.fetch_add(1, std::memory_order_relaxed);
        DbShard& shard = get_shard(key);
        int64_t now = TimeUtils::now_ms();
        UniqueLock lock(shard.mutex());
        ValueObject* obj = shard.find_unlocked(key, now);

        if (!obj) {
            ValueObject new_obj = ValueObject::make_hash();
            new_obj.hash_val[field] = val;
            shard.set_unlocked(key, std::move(new_obj));
            return 1; // 1 = new field added
        }

        if (obj->type != DataType::HASH) {
            return -1; // Wrong type
        }

        auto it = obj->hash_val.find(field);
        bool is_new = (it == obj->hash_val.end());
        obj->hash_val[field] = val;
        return is_new ? 1 : 0;
    }

    int hget(const std::string& key, const std::string& field, std::string& out_val) {
        total_reads_.fetch_add(1, std::memory_order_relaxed);
        DbShard& shard = get_shard(key);
        int64_t now = TimeUtils::now_ms();
        SharedLock lock(shard.mutex());
        ValueObject* obj = shard.find_unlocked(key, now);

        if (!obj) return 0; // Key not found
        if (obj->type != DataType::HASH) return -1; // Wrong type

        auto it = obj->hash_val.find(field);
        if (it == obj->hash_val.end()) return 0; // Field not found

        out_val = it->second;
        return 1;
    }

    int hdel(const std::string& key, const std::vector<std::string>& fields) {
        total_writes_.fetch_add(1, std::memory_order_relaxed);
        DbShard& shard = get_shard(key);
        int64_t now = TimeUtils::now_ms();
        UniqueLock lock(shard.mutex());
        ValueObject* obj = shard.find_unlocked(key, now);

        if (!obj) return 0;
        if (obj->type != DataType::HASH) return -1;

        int deleted = 0;
        for (const auto& f : fields) {
            if (obj->hash_val.erase(f) > 0) {
                deleted++;
            }
        }
        return deleted;
    }

    int hgetall(const std::string& key, std::vector<std::pair<std::string, std::string>>& out_pairs) {
        total_reads_.fetch_add(1, std::memory_order_relaxed);
        DbShard& shard = get_shard(key);
        int64_t now = TimeUtils::now_ms();
        SharedLock lock(shard.mutex());
        ValueObject* obj = shard.find_unlocked(key, now);

        if (!obj) return 0;
        if (obj->type != DataType::HASH) return -1;

        out_pairs.reserve(obj->hash_val.size());
        for (const auto& kv : obj->hash_val) {
            out_pairs.emplace_back(kv.first, kv.second);
        }
        return 1;
    }

    int64_t hlen(const std::string& key) {
        DbShard& shard = get_shard(key);
        int64_t now = TimeUtils::now_ms();
        SharedLock lock(shard.mutex());
        ValueObject* obj = shard.find_unlocked(key, now);
        if (!obj) return 0;
        if (obj->type != DataType::HASH) return -1;
        return static_cast<int64_t>(obj->hash_val.size());
    }

    // ==========================================
    // List Operations
    // ==========================================
    int64_t lpush(const std::string& key, const std::vector<std::string>& values) {
        total_writes_.fetch_add(1, std::memory_order_relaxed);
        DbShard& shard = get_shard(key);
        int64_t now = TimeUtils::now_ms();
        UniqueLock lock(shard.mutex());
        ValueObject* obj = shard.find_unlocked(key, now);

        if (!obj) {
            ValueObject new_obj = ValueObject::make_list();
            for (const auto& v : values) {
                new_obj.list_val.push_front(v);
            }
            size_t sz = new_obj.list_val.size();
            shard.set_unlocked(key, std::move(new_obj));
            return static_cast<int64_t>(sz);
        }

        if (obj->type != DataType::LIST) return -1;

        for (const auto& v : values) {
            obj->list_val.push_front(v);
        }
        return static_cast<int64_t>(obj->list_val.size());
    }

    int64_t rpush(const std::string& key, const std::vector<std::string>& values) {
        total_writes_.fetch_add(1, std::memory_order_relaxed);
        DbShard& shard = get_shard(key);
        int64_t now = TimeUtils::now_ms();
        UniqueLock lock(shard.mutex());
        ValueObject* obj = shard.find_unlocked(key, now);

        if (!obj) {
            ValueObject new_obj = ValueObject::make_list();
            for (const auto& v : values) {
                new_obj.list_val.push_back(v);
            }
            size_t sz = new_obj.list_val.size();
            shard.set_unlocked(key, std::move(new_obj));
            return static_cast<int64_t>(sz);
        }

        if (obj->type != DataType::LIST) return -1;

        for (const auto& v : values) {
            obj->list_val.push_back(v);
        }
        return static_cast<int64_t>(obj->list_val.size());
    }

    int lpop(const std::string& key, std::string& out_val) {
        total_writes_.fetch_add(1, std::memory_order_relaxed);
        DbShard& shard = get_shard(key);
        int64_t now = TimeUtils::now_ms();
        UniqueLock lock(shard.mutex());
        ValueObject* obj = shard.find_unlocked(key, now);

        if (!obj) return 0;
        if (obj->type != DataType::LIST) return -1;
        if (obj->list_val.empty()) return 0;

        out_val = std::move(obj->list_val.front());
        obj->list_val.pop_front();
        return 1;
    }

    int rpop(const std::string& key, std::string& out_val) {
        total_writes_.fetch_add(1, std::memory_order_relaxed);
        DbShard& shard = get_shard(key);
        int64_t now = TimeUtils::now_ms();
        UniqueLock lock(shard.mutex());
        ValueObject* obj = shard.find_unlocked(key, now);

        if (!obj) return 0;
        if (obj->type != DataType::LIST) return -1;
        if (obj->list_val.empty()) return 0;

        out_val = std::move(obj->list_val.back());
        obj->list_val.pop_back();
        return 1;
    }

    int64_t llen(const std::string& key) {
        DbShard& shard = get_shard(key);
        int64_t now = TimeUtils::now_ms();
        SharedLock lock(shard.mutex());
        ValueObject* obj = shard.find_unlocked(key, now);
        if (!obj) return 0;
        if (obj->type != DataType::LIST) return -1;
        return static_cast<int64_t>(obj->list_val.size());
    }

    int lrange(const std::string& key, int64_t start, int64_t stop, std::vector<std::string>& out_elems) {
        DbShard& shard = get_shard(key);
        int64_t now = TimeUtils::now_ms();
        SharedLock lock(shard.mutex());
        ValueObject* obj = shard.find_unlocked(key, now);

        if (!obj) return 0;
        if (obj->type != DataType::LIST) return -1;

        int64_t sz = static_cast<int64_t>(obj->list_val.size());
        if (start < 0) start = sz + start;
        if (stop < 0) stop = sz + stop;
        if (start < 0) start = 0;
        if (stop >= sz) stop = sz - 1;

        if (start > stop || start >= sz) {
            return 1; // Empty range
        }

        for (int64_t i = start; i <= stop; ++i) {
            out_elems.push_back(obj->list_val[static_cast<size_t>(i)]);
        }
        return 1;
    }

    // ==========================================
    // Set Operations
    // ==========================================
    int64_t sadd(const std::string& key, const std::vector<std::string>& members) {
        total_writes_.fetch_add(1, std::memory_order_relaxed);
        DbShard& shard = get_shard(key);
        int64_t now = TimeUtils::now_ms();
        UniqueLock lock(shard.mutex());
        ValueObject* obj = shard.find_unlocked(key, now);

        if (!obj) {
            ValueObject new_obj = ValueObject::make_set();
            int64_t added = 0;
            for (const auto& m : members) {
                if (new_obj.set_val.insert(m).second) {
                    added++;
                }
            }
            shard.set_unlocked(key, std::move(new_obj));
            return added;
        }

        if (obj->type != DataType::SET) return -1;

        int64_t added = 0;
        for (const auto& m : members) {
            if (obj->set_val.insert(m).second) {
                added++;
            }
        }
        return added;
    }

    int64_t srem(const std::string& key, const std::vector<std::string>& members) {
        total_writes_.fetch_add(1, std::memory_order_relaxed);
        DbShard& shard = get_shard(key);
        int64_t now = TimeUtils::now_ms();
        UniqueLock lock(shard.mutex());
        ValueObject* obj = shard.find_unlocked(key, now);

        if (!obj) return 0;
        if (obj->type != DataType::SET) return -1;

        int64_t removed = 0;
        for (const auto& m : members) {
            if (obj->set_val.erase(m) > 0) {
                removed++;
            }
        }
        return removed;
    }

    int smembers(const std::string& key, std::vector<std::string>& out_members) {
        DbShard& shard = get_shard(key);
        int64_t now = TimeUtils::now_ms();
        SharedLock lock(shard.mutex());
        ValueObject* obj = shard.find_unlocked(key, now);

        if (!obj) return 0;
        if (obj->type != DataType::SET) return -1;

        out_members.reserve(obj->set_val.size());
        for (const auto& m : obj->set_val) {
            out_members.push_back(m);
        }
        return 1;
    }

    int sismember(const std::string& key, const std::string& member) {
        DbShard& shard = get_shard(key);
        int64_t now = TimeUtils::now_ms();
        SharedLock lock(shard.mutex());
        ValueObject* obj = shard.find_unlocked(key, now);

        if (!obj) return 0;
        if (obj->type != DataType::SET) return -1;

        return obj->set_val.count(member) ? 1 : 0;
    }

    int64_t scard(const std::string& key) {
        DbShard& shard = get_shard(key);
        int64_t now = TimeUtils::now_ms();
        SharedLock lock(shard.mutex());
        ValueObject* obj = shard.find_unlocked(key, now);
        if (!obj) return 0;
        if (obj->type != DataType::SET) return -1;
        return static_cast<int64_t>(obj->set_val.size());
    }

    // ==========================================
    // TTL / Expiration Operations
    // ==========================================
    bool expire(const std::string& key, int64_t seconds) {
        DbShard& shard = get_shard(key);
        int64_t now = TimeUtils::now_ms();
        UniqueLock lock(shard.mutex());
        ValueObject* obj = shard.find_unlocked(key, now);
        if (!obj) return false;

        obj->expire_at_ms = now + (seconds * 1000);
        return true;
    }

    int64_t ttl(const std::string& key) {
        DbShard& shard = get_shard(key);
        int64_t now = TimeUtils::now_ms();
        SharedLock lock(shard.mutex());
        ValueObject* obj = shard.find_unlocked(key, now);
        if (!obj) return -2; // Key does not exist
        return obj->ttl_seconds(now);
    }

    bool persist(const std::string& key) {
        DbShard& shard = get_shard(key);
        int64_t now = TimeUtils::now_ms();
        UniqueLock lock(shard.mutex());
        ValueObject* obj = shard.find_unlocked(key, now);
        if (!obj || obj->expire_at_ms == 0) return false;

        obj->expire_at_ms = 0;
        return true;
    }

    // ==========================================
    // Server Stats & Management
    // ==========================================
    size_t dbsize() {
        size_t total = 0;
        int64_t now = TimeUtils::now_ms();
        for (size_t i = 0; i < num_shards_; ++i) {
            SharedLock lock(shards_[i].mutex());
            for (const auto& pair : shards_[i].entries_unlocked()) {
                if (!pair.second.is_expired(now)) {
                    total++;
                }
            }
        }
        return total;
    }

    void flushdb() {
        for (size_t i = 0; i < num_shards_; ++i) {
            UniqueLock lock(shards_[i].mutex());
            shards_[i].clear_unlocked();
        }
    }

    std::vector<std::string> get_all_keys() {
        std::vector<std::string> keys;
        int64_t now = TimeUtils::now_ms();
        for (size_t i = 0; i < num_shards_; ++i) {
            SharedLock lock(shards_[i].mutex());
            for (const auto& pair : shards_[i].entries_unlocked()) {
                if (!pair.second.is_expired(now)) {
                    keys.push_back(pair.first);
                }
            }
        }
        return keys;
    }

    // Active eviction cycle: samples random shards
    size_t run_active_expiry_cycle(size_t max_shards_to_check = 16, size_t sample_size = 20) {
        size_t total_purged = 0;
        int64_t now = TimeUtils::now_ms();
        for (size_t i = 0; i < max_shards_to_check; ++i) {
            size_t idx = static_cast<size_t>(std::rand()) % num_shards_;
            total_purged += shards_[idx].purge_expired_sample(sample_size, now);
        }
        return total_purged;
    }

    uint64_t total_reads() const { return total_reads_.load(); }
    uint64_t total_writes() const { return total_writes_.load(); }
    void increment_commands() { total_commands_.fetch_add(1, std::memory_order_relaxed); }
    uint64_t total_commands() const { return total_commands_.load(); }

private:
    size_t num_shards_;
    size_t mask_;
    DbShard* shards_;

    std::atomic<uint64_t> total_commands_;
    std::atomic<uint64_t> total_reads_;
    std::atomic<uint64_t> total_writes_;

    StorageEngine(const StorageEngine&) = delete;
    StorageEngine& operator=(const StorageEngine&) = delete;
};

} // namespace flashkv
