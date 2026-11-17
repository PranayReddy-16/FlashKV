#pragma once

#include <string>
#include <vector>
#include <fstream>
#include <iostream>
#include <cstdint>
#include <cstring>
#include "storage/engine.hpp"
#include "common/types.hpp"
#include "common/time_utils.hpp"

namespace flashkv {

class RdbSnapshot {
public:
    static constexpr const char* MAGIC = "FLASHKV";
    static constexpr uint8_t VERSION = 1;

    static bool save(StorageEngine& engine, const std::string& filepath = "dump.rdb") {
        std::string tmp_path = filepath + ".tmp";
        std::ofstream out(tmp_path, std::ios::binary);
        if (!out.is_open()) {
            return false;
        }

        // Write Header
        out.write("FLASHKV", 7);
        uint8_t ver = VERSION;
        out.write(reinterpret_cast<const char*>(&ver), sizeof(ver));

        int64_t now = TimeUtils::now_ms();

        // Count valid keys first
        uint64_t key_count = 0;
        for (size_t s = 0; s < engine.shard_count(); ++s) {
            DbShard& shard = engine.get_shard_by_index(s);
            SharedLock lock(shard.mutex());
            for (const auto& pair : shard.entries_unlocked()) {
                if (!pair.second.is_expired(now)) {
                    key_count++;
                }
            }
        }
        out.write(reinterpret_cast<const char*>(&key_count), sizeof(key_count));

        // Write each key and value
        for (size_t s = 0; s < engine.shard_count(); ++s) {
            DbShard& shard = engine.get_shard_by_index(s);
            SharedLock lock(shard.mutex());
            for (const auto& pair : shard.entries_unlocked()) {
                const std::string& key = pair.first;
                const ValueObject& obj = pair.second;
                if (obj.is_expired(now)) continue;

                // Write key
                uint32_t klen = static_cast<uint32_t>(key.size());
                out.write(reinterpret_cast<const char*>(&klen), sizeof(klen));
                out.write(key.data(), klen);

                // Write type and expiry
                uint8_t type_byte = static_cast<uint8_t>(obj.type);
                out.write(reinterpret_cast<const char*>(&type_byte), sizeof(type_byte));
                out.write(reinterpret_cast<const char*>(&obj.expire_at_ms), sizeof(obj.expire_at_ms));

                // Write payload
                if (obj.type == DataType::STRING) {
                    uint32_t slen = static_cast<uint32_t>(obj.string_val.size());
                    out.write(reinterpret_cast<const char*>(&slen), sizeof(slen));
                    out.write(obj.string_val.data(), slen);
                } else if (obj.type == DataType::HASH) {
                    uint32_t num_fields = static_cast<uint32_t>(obj.hash_val.size());
                    out.write(reinterpret_cast<const char*>(&num_fields), sizeof(num_fields));
                    for (const auto& kv : obj.hash_val) {
                        uint32_t flen = static_cast<uint32_t>(kv.first.size());
                        out.write(reinterpret_cast<const char*>(&flen), sizeof(flen));
                        out.write(kv.first.data(), flen);

                        uint32_t vlen = static_cast<uint32_t>(kv.second.size());
                        out.write(reinterpret_cast<const char*>(&vlen), sizeof(vlen));
                        out.write(kv.second.data(), vlen);
                    }
                } else if (obj.type == DataType::LIST) {
                    uint32_t num_items = static_cast<uint32_t>(obj.list_val.size());
                    out.write(reinterpret_cast<const char*>(&num_items), sizeof(num_items));
                    for (const auto& item : obj.list_val) {
                        uint32_t ilen = static_cast<uint32_t>(item.size());
                        out.write(reinterpret_cast<const char*>(&ilen), sizeof(ilen));
                        out.write(item.data(), ilen);
                    }
                } else if (obj.type == DataType::SET) {
                    uint32_t num_items = static_cast<uint32_t>(obj.set_val.size());
                    out.write(reinterpret_cast<const char*>(&num_items), sizeof(num_items));
                    for (const auto& item : obj.set_val) {
                        uint32_t ilen = static_cast<uint32_t>(item.size());
                        out.write(reinterpret_cast<const char*>(&ilen), sizeof(ilen));
                        out.write(item.data(), ilen);
                    }
                }
            }
        }

        out.close();
        std::remove(filepath.c_str());
        return std::rename(tmp_path.c_str(), filepath.c_str()) == 0;
    }

    static size_t load(StorageEngine& engine, const std::string& filepath = "dump.rdb") {
        std::ifstream in(filepath, std::ios::binary);
        if (!in.is_open()) {
            return 0;
        }

        char magic[8] = {0};
        in.read(magic, 7);
        if (std::strcmp(magic, MAGIC) != 0) {
            std::cerr << "[RDB] Error: Invalid RDB magic header!" << std::endl;
            return 0;
        }

        uint8_t version = 0;
        in.read(reinterpret_cast<char*>(&version), sizeof(version));
        if (version != VERSION) {
            std::cerr << "[RDB] Error: Unsupported RDB version " << (int)version << std::endl;
            return 0;
        }

        uint64_t key_count = 0;
        in.read(reinterpret_cast<char*>(&key_count), sizeof(key_count));

        int64_t now = TimeUtils::now_ms();
        size_t loaded = 0;

        for (uint64_t i = 0; i < key_count && in.good(); ++i) {
            uint32_t klen = 0;
            in.read(reinterpret_cast<char*>(&klen), sizeof(klen));
            std::string key(klen, '\0');
            in.read(&key[0], klen);

            uint8_t type_byte = 0;
            in.read(reinterpret_cast<char*>(&type_byte), sizeof(type_byte));
            int64_t expire_at_ms = 0;
            in.read(reinterpret_cast<char*>(&expire_at_ms), sizeof(expire_at_ms));

            ValueObject obj;
            obj.type = static_cast<DataType>(type_byte);
            obj.expire_at_ms = expire_at_ms;

            if (obj.type == DataType::STRING) {
                uint32_t slen = 0;
                in.read(reinterpret_cast<char*>(&slen), sizeof(slen));
                obj.string_val.resize(slen);
                in.read(&obj.string_val[0], slen);
            } else if (obj.type == DataType::HASH) {
                uint32_t num_fields = 0;
                in.read(reinterpret_cast<char*>(&num_fields), sizeof(num_fields));
                for (uint32_t f = 0; f < num_fields; ++f) {
                    uint32_t flen = 0;
                    in.read(reinterpret_cast<char*>(&flen), sizeof(flen));
                    std::string field(flen, '\0');
                    in.read(&field[0], flen);

                    uint32_t vlen = 0;
                    in.read(reinterpret_cast<char*>(&vlen), sizeof(vlen));
                    std::string val(vlen, '\0');
                    in.read(&val[0], vlen);

                    obj.hash_val[field] = std::move(val);
                }
            } else if (obj.type == DataType::LIST) {
                uint32_t num_items = 0;
                in.read(reinterpret_cast<char*>(&num_items), sizeof(num_items));
                for (uint32_t item = 0; item < num_items; ++item) {
                    uint32_t ilen = 0;
                    in.read(reinterpret_cast<char*>(&ilen), sizeof(ilen));
                    std::string str(ilen, '\0');
                    in.read(&str[0], ilen);
                    obj.list_val.push_back(std::move(str));
                }
            } else if (obj.type == DataType::SET) {
                uint32_t num_items = 0;
                in.read(reinterpret_cast<char*>(&num_items), sizeof(num_items));
                for (uint32_t item = 0; item < num_items; ++item) {
                    uint32_t ilen = 0;
                    in.read(reinterpret_cast<char*>(&ilen), sizeof(ilen));
                    std::string str(ilen, '\0');
                    in.read(&str[0], ilen);
                    obj.set_val.insert(std::move(str));
                }
            }

            if (!obj.is_expired(now)) {
                DbShard& shard = engine.get_shard(key);
                UniqueLock lock(shard.mutex());
                shard.set_unlocked(key, std::move(obj));
                loaded++;
            }
        }

        return loaded;
    }
};

} // namespace flashkv
