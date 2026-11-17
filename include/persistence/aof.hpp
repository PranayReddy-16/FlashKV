#pragma once

#include <string>
#include <vector>
#include <fstream>
#include <iostream>
#include <cstdio>
#include <cstdint>
#include "common/concurrency.hpp"
#include "protocol/resp.hpp"
#include "protocol/command_parser.hpp"
#include "storage/engine.hpp"

#ifdef _WIN32
  #include <io.h>
  #include <stdio.h>
#else
  #include <unistd.h>
#endif

namespace flashkv {

inline void sync_file_to_disk(std::FILE* fp) {
    if (!fp) return;
    std::fflush(fp);
#ifdef _WIN32
    _commit(_fileno(fp));
#else
    fsync(fileno(fp));
#endif
}

enum class AofSyncPolicy {
    ALWAYS,
    EVERYSEC,
    NO
};

class AofManager {
public:
    explicit AofManager(StorageEngine& engine,
                       const std::string& filepath = "appendonly.aof",
                       AofSyncPolicy sync_policy = AofSyncPolicy::EVERYSEC)
        : engine_(engine), filepath_(filepath), sync_policy_(sync_policy),
          enabled_(true), running_(false), fd_(-1) {
    }

    ~AofManager() {
        stop();
    }

    bool start() {
        LockGuard lock(mutex_);
        if (running_) return true;

        // Open AOF file in append mode
        fp_ = std::fopen(filepath_.c_str(), "ab");
        if (!fp_) {
            std::cerr << "[AOF] Warning: Could not open AOF file: " << filepath_ << std::endl;
            return false;
        }

        running_ = true;
        // Start background flusher thread if EVERYSEC
        if (sync_policy_ == AofSyncPolicy::EVERYSEC) {
            flusher_thread_ = Thread([this]() {
                this->flusher_loop();
            });
        }
        return true;
    }

    void stop() {
        {
            LockGuard lock(mutex_);
            if (!running_) return;
            running_ = false;
        }

        if (flusher_thread_.joinable()) {
            flusher_thread_.join();
        }

        LockGuard lock(mutex_);
        if (fp_) {
            std::fflush(fp_);
            std::fclose(fp_);
            fp_ = nullptr;
        }
    }

    // Append a mutating command to AOF in standard RESP format
    void append_command(const std::vector<std::string>& args) {
        if (!enabled_ || !running_ || args.empty()) return;

        // Construct RESP array representation
        std::vector<RespValue> elements;
        elements.reserve(args.size());
        for (const auto& arg : args) {
            elements.push_back(RespValue::make_bulk_string(arg));
        }
        RespValue array_val = RespValue::make_array(elements);
        std::string serialized = array_val.serialize();

        LockGuard lock(mutex_);
        if (!fp_) return;

        std::fwrite(serialized.data(), 1, serialized.size(), fp_);

        if (sync_policy_ == AofSyncPolicy::ALWAYS) {
            sync_file_to_disk(fp_);
        }
    }

    // Replay AOF on startup: restores database state
    size_t replay() {
        std::FILE* fp = std::fopen(filepath_.c_str(), "rb");
        if (!fp) {
            return 0; // No existing AOF file
        }

        std::string buffer;
        char chunk[8192];
        size_t nread = 0;
        size_t commands_replayed = 0;

        while ((nread = std::fread(chunk, 1, sizeof(chunk), fp)) > 0) {
            buffer.append(chunk, nread);
            std::vector<std::string> args;
            std::string err;

            while (true) {
                auto status = CommandParser::parse(buffer, args, err);
                if (status == CommandParser::ParseStatus::SUCCESS) {
                    execute_replay_command(args);
                    commands_replayed++;
                    args.clear();
                } else {
                    break;
                }
            }
        }

        std::fclose(fp);
        return commands_replayed;
    }

    // Rewrite / Compact AOF: snapshots current state into a compact file
    bool rewrite_aof() {
        std::string tmp_file = filepath_ + ".tmp";
        std::FILE* tmp_fp = std::fopen(tmp_file.c_str(), "wb");
        if (!tmp_fp) {
            return false;
        }

        int64_t now = TimeUtils::now_ms();

        // Iterate over all shards and serialize live keys
        for (size_t s = 0; s < engine_.shard_count(); ++s) {
            DbShard& shard = engine_.get_shard_by_index(s);
            SharedLock lock(shard.mutex());

            for (const auto& pair : shard.entries_unlocked()) {
                const std::string& key = pair.first;
                const ValueObject& obj = pair.second;

                if (obj.is_expired(now)) continue;

                int64_t ttl_sec = obj.ttl_seconds(now);

                if (obj.type == DataType::STRING) {
                    std::vector<RespValue> cmd = {
                        RespValue::make_bulk_string("SET"),
                        RespValue::make_bulk_string(key),
                        RespValue::make_bulk_string(obj.string_val)
                    };
                    if (ttl_sec > 0) {
                        cmd.push_back(RespValue::make_bulk_string("EX"));
                        cmd.push_back(RespValue::make_bulk_string(std::to_string(ttl_sec)));
                    }
                    std::string s = RespValue::make_array(cmd).serialize();
                    std::fwrite(s.data(), 1, s.size(), tmp_fp);
                } else if (obj.type == DataType::HASH) {
                    for (const auto& kv : obj.hash_val) {
                        std::vector<RespValue> cmd = {
                            RespValue::make_bulk_string("HSET"),
                            RespValue::make_bulk_string(key),
                            RespValue::make_bulk_string(kv.first),
                            RespValue::make_bulk_string(kv.second)
                        };
                        std::string s = RespValue::make_array(cmd).serialize();
                        std::fwrite(s.data(), 1, s.size(), tmp_fp);
                    }
                    if (ttl_sec > 0) {
                        std::vector<RespValue> exp_cmd = {
                            RespValue::make_bulk_string("EXPIRE"),
                            RespValue::make_bulk_string(key),
                            RespValue::make_bulk_string(std::to_string(ttl_sec))
                        };
                        std::string s = RespValue::make_array(exp_cmd).serialize();
                        std::fwrite(s.data(), 1, s.size(), tmp_fp);
                    }
                } else if (obj.type == DataType::LIST) {
                    if (!obj.list_val.empty()) {
                        std::vector<RespValue> cmd = {
                            RespValue::make_bulk_string("RPUSH"),
                            RespValue::make_bulk_string(key)
                        };
                        for (const auto& item : obj.list_val) {
                            cmd.push_back(RespValue::make_bulk_string(item));
                        }
                        std::string s = RespValue::make_array(cmd).serialize();
                        std::fwrite(s.data(), 1, s.size(), tmp_fp);

                        if (ttl_sec > 0) {
                            std::vector<RespValue> exp_cmd = {
                                RespValue::make_bulk_string("EXPIRE"),
                                RespValue::make_bulk_string(key),
                                RespValue::make_bulk_string(std::to_string(ttl_sec))
                            };
                            std::string es = RespValue::make_array(exp_cmd).serialize();
                            std::fwrite(es.data(), 1, es.size(), tmp_fp);
                        }
                    }
                } else if (obj.type == DataType::SET) {
                    if (!obj.set_val.empty()) {
                        std::vector<RespValue> cmd = {
                            RespValue::make_bulk_string("SADD"),
                            RespValue::make_bulk_string(key)
                        };
                        for (const auto& item : obj.set_val) {
                            cmd.push_back(RespValue::make_bulk_string(item));
                        }
                        std::string s = RespValue::make_array(cmd).serialize();
                        std::fwrite(s.data(), 1, s.size(), tmp_fp);

                        if (ttl_sec > 0) {
                            std::vector<RespValue> exp_cmd = {
                                RespValue::make_bulk_string("EXPIRE"),
                                RespValue::make_bulk_string(key),
                                RespValue::make_bulk_string(std::to_string(ttl_sec))
                            };
                            std::string es = RespValue::make_array(exp_cmd).serialize();
                            std::fwrite(es.data(), 1, es.size(), tmp_fp);
                        }
                    }
                }
            }
        }

        std::fflush(tmp_fp);
        std::fclose(tmp_fp);

        // Atomically replace existing AOF file
        LockGuard lock(mutex_);
        if (fp_) {
            std::fclose(fp_);
            fp_ = nullptr;
        }

        std::remove(filepath_.c_str());
        std::rename(tmp_file.c_str(), filepath_.c_str());

        // Reopen AOF
        fp_ = std::fopen(filepath_.c_str(), "ab");
        return true;
    }

private:
    void flusher_loop() {
        while (running_) {
            Thread::sleep_ms(1000);
            LockGuard lock(mutex_);
            if (!running_) break;
            if (fp_) {
                sync_file_to_disk(fp_);
            }
        }
    }

    void execute_replay_command(const std::vector<std::string>& args) {
        if (args.empty()) return;
        const std::string& cmd = args[0];

        if (cmd == "SET" && args.size() >= 3) {
            int64_t expire_at = 0;
            if (args.size() >= 5 && (args[3] == "EX" || args[3] == "ex")) {
                int64_t sec = std::strtoll(args[4].c_str(), nullptr, 10);
                if (sec > 0) expire_at = TimeUtils::now_ms() + sec * 1000;
            }
            engine_.set(args[1], args[2], expire_at);
        } else if (cmd == "SETEX" && args.size() >= 4) {
            int64_t sec = std::strtoll(args[2].c_str(), nullptr, 10);
            int64_t expire_at = TimeUtils::now_ms() + sec * 1000;
            engine_.set(args[1], args[3], expire_at);
        } else if (cmd == "INCR" && args.size() >= 2) {
            int64_t res = 0;
            std::string err;
            engine_.incr_by(args[1], 1, res, err);
        } else if (cmd == "DECR" && args.size() >= 2) {
            int64_t res = 0;
            std::string err;
            engine_.incr_by(args[1], -1, res, err);
        } else if (cmd == "INCRBY" && args.size() >= 3) {
            int64_t delta = std::strtoll(args[2].c_str(), nullptr, 10);
            int64_t res = 0;
            std::string err;
            engine_.incr_by(args[1], delta, res, err);
        } else if (cmd == "DEL" && args.size() >= 2) {
            for (size_t i = 1; i < args.size(); ++i) {
                engine_.del(args[i]);
            }
        } else if (cmd == "HSET" && args.size() >= 4) {
            engine_.hset(args[1], args[2], args[3]);
        } else if (cmd == "HDEL" && args.size() >= 3) {
            std::vector<std::string> fields(args.begin() + 2, args.end());
            engine_.hdel(args[1], fields);
        } else if (cmd == "LPUSH" && args.size() >= 3) {
            std::vector<std::string> vals(args.begin() + 2, args.end());
            engine_.lpush(args[1], vals);
        } else if (cmd == "RPUSH" && args.size() >= 3) {
            std::vector<std::string> vals(args.begin() + 2, args.end());
            engine_.rpush(args[1], vals);
        } else if (cmd == "SADD" && args.size() >= 3) {
            std::vector<std::string> members(args.begin() + 2, args.end());
            engine_.sadd(args[1], members);
        } else if (cmd == "SREM" && args.size() >= 3) {
            std::vector<std::string> members(args.begin() + 2, args.end());
            engine_.srem(args[1], members);
        } else if (cmd == "EXPIRE" && args.size() >= 3) {
            int64_t sec = std::strtoll(args[2].c_str(), nullptr, 10);
            engine_.expire(args[1], sec);
        } else if (cmd == "FLUSHDB") {
            engine_.flushdb();
        }
    }

    StorageEngine& engine_;
    std::string filepath_;
    AofSyncPolicy sync_policy_;
    bool enabled_;
    bool running_;
    int fd_;
    std::FILE* fp_ = nullptr;
    Mutex mutex_;
    Thread flusher_thread_;
};

} // namespace flashkv
