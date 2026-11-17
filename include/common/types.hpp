#pragma once

#include <string>
#include <vector>
#include <deque>
#include <unordered_map>
#include <unordered_set>
#include <cstdint>
#include "common/time_utils.hpp"

namespace flashkv {

enum class DataType : uint8_t {
    NONE = 0,
    STRING = 1,
    HASH = 2,
    LIST = 3,
    SET = 4
};

inline const char* data_type_to_string(DataType type) {
    switch (type) {
        case DataType::STRING: return "string";
        case DataType::HASH:   return "hash";
        case DataType::LIST:   return "list";
        case DataType::SET:    return "set";
        default:               return "none";
    }
}

struct ValueObject {
    DataType type;
    int64_t expire_at_ms; // 0 means persistent (no expiry)

    // Data payloads
    std::string string_val;
    std::unordered_map<std::string, std::string> hash_val;
    std::deque<std::string> list_val;
    std::unordered_set<std::string> set_val;

    ValueObject() : type(DataType::NONE), expire_at_ms(0) {}

    static ValueObject make_string(const std::string& val, int64_t expire_at = 0) {
        ValueObject obj;
        obj.type = DataType::STRING;
        obj.expire_at_ms = expire_at;
        obj.string_val = val;
        return obj;
    }

    static ValueObject make_hash(int64_t expire_at = 0) {
        ValueObject obj;
        obj.type = DataType::HASH;
        obj.expire_at_ms = expire_at;
        return obj;
    }

    static ValueObject make_list(int64_t expire_at = 0) {
        ValueObject obj;
        obj.type = DataType::LIST;
        obj.expire_at_ms = expire_at;
        return obj;
    }

    static ValueObject make_set(int64_t expire_at = 0) {
        ValueObject obj;
        obj.type = DataType::SET;
        obj.expire_at_ms = expire_at;
        return obj;
    }

    bool is_expired(int64_t current_time_ms = 0) const {
        if (expire_at_ms == 0) return false;
        if (current_time_ms == 0) {
            current_time_ms = TimeUtils::now_ms();
        }
        return current_time_ms >= expire_at_ms;
    }

    // TTL in seconds: -1 if no expiry, -2 if expired
    int64_t ttl_seconds(int64_t current_time_ms = 0) const {
        if (expire_at_ms == 0) return -1;
        if (current_time_ms == 0) {
            current_time_ms = TimeUtils::now_ms();
        }
        if (current_time_ms >= expire_at_ms) return -2;
        int64_t diff = expire_at_ms - current_time_ms;
        return (diff + 999) / 1000; // Round up
    }
};

} // namespace flashkv
