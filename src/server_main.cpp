#include <iostream>
#include <string>
#include <cstdlib>
#include "server/flash_server.hpp"

using namespace flashkv;

void print_help(const char* prog) {
    std::cout << "Usage: " << prog << " [options]\n"
              << "Options:\n"
              << "  -p, --port <port>         TCP port to listen on (default: 6379)\n"
              << "  -h, --host <ip>           Host IP to bind to (default: 127.0.0.1)\n"
              << "  -t, --threads <num>       Worker thread count (default: 4)\n"
              << "  -s, --shards <num>        Storage engine shard count (default: 64)\n"
              << "  --no-aof                  Disable Append-Only File persistence\n"
              << "  --aof-path <path>         AOF log file path (default: appendonly.aof)\n"
              << "  --no-rdb                  Disable RDB snapshot persistence\n"
              << "  --rdb-path <path>         RDB snapshot file path (default: dump.rdb)\n"
              << "  -v, --verbose             Enable verbose client logging\n"
              << "  --help                    Show this help message\n";
}

int main(int argc, char* argv[]) {
    ServerConfig config;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "-p" || arg == "--port") {
            if (i + 1 < argc) config.port = std::atoi(argv[++i]);
        } else if (arg == "-h" || arg == "--host") {
            if (i + 1 < argc) config.host = argv[++i];
        } else if (arg == "-t" || arg == "--threads") {
            if (i + 1 < argc) config.worker_threads = static_cast<size_t>(std::atoi(argv[++i]));
        } else if (arg == "-s" || arg == "--shards") {
            if (i + 1 < argc) config.num_shards = static_cast<size_t>(std::atoi(argv[++i]));
        } else if (arg == "--no-aof") {
            config.enable_aof = false;
        } else if (arg == "--aof-path") {
            if (i + 1 < argc) config.aof_path = argv[++i];
        } else if (arg == "--no-rdb") {
            config.enable_rdb = false;
        } else if (arg == "--rdb-path") {
            if (i + 1 < argc) config.rdb_path = argv[++i];
        } else if (arg == "-v" || arg == "--verbose") {
            config.verbose = true;
        } else if (arg == "--help") {
            print_help(argv[0]);
            return 0;
        }
    }

    FlashServer server(config);
    if (!server.start()) {
        return 1;
    }

    return 0;
}
