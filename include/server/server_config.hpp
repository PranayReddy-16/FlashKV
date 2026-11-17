#pragma once

#include <string>

namespace flashkv {

struct ServerConfig {
    std::string host = "127.0.0.1";
    int port = 6379;
    size_t worker_threads = 4;
    size_t num_shards = 64;
    bool enable_aof = true;
    std::string aof_path = "appendonly.aof";
    bool enable_rdb = true;
    std::string rdb_path = "dump.rdb";
    bool verbose = false;
};

} // namespace flashkv
