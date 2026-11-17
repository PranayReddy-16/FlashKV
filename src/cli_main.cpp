#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include "net/socket.hpp"
#include "net/tcp_client.hpp"

using namespace flashkv;

void print_resp_value(const RespValue& resp, int indent = 0) {
    std::string pad(indent * 2, ' ');
    switch (resp.type) {
        case RespType::SIMPLE_STRING:
            std::cout << pad << resp.str_val << "\n";
            break;
        case RespType::ERR:
            std::cout << pad << "(error) " << resp.str_val << "\n";
            break;
        case RespType::INTEGER:
            std::cout << pad << "(integer) " << resp.int_val << "\n";
            break;
        case RespType::BULK_STRING:
            std::cout << pad << "\"" << resp.str_val << "\"\n";
            break;
        case RespType::NULL_BULK_STRING:
        case RespType::NULL_ARRAY:
            std::cout << pad << "(nil)\n";
            break;
        case RespType::ARRAY:
            if (resp.elements.empty()) {
                std::cout << pad << "(empty array)\n";
            } else {
                for (size_t i = 0; i < resp.elements.size(); ++i) {
                    std::cout << pad << (i + 1) << ") ";
                    print_resp_value(resp.elements[i], indent + 1);
                }
            }
            break;
    }
}

int main(int argc, char* argv[]) {
    SocketPlatform::init();

    std::string host = "127.0.0.1";
    int port = 6379;
    std::vector<std::string> one_shot_cmd;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "-h" && i + 1 < argc) {
            host = argv[++i];
        } else if (arg == "-p" && i + 1 < argc) {
            port = std::atoi(argv[++i]);
        } else {
            // Remaining args are a one-shot command
            while (i < argc) {
                one_shot_cmd.push_back(argv[i++]);
            }
            break;
        }
    }

    TcpClient client;
    if (!client.connect(host, port)) {
        std::cerr << "Could not connect to FlashKV at " << host << ":" << port << "\n";
        SocketPlatform::cleanup();
        return 1;
    }

    // If one-shot command passed on command line
    if (!one_shot_cmd.empty()) {
        RespValue resp;
        std::string err;
        if (client.send_command(one_shot_cmd, resp, err)) {
            print_resp_value(resp);
        } else {
            std::cerr << "Error: " << err << "\n";
        }
        client.disconnect();
        SocketPlatform::cleanup();
        return 0;
    }

    // Interactive REPL Mode
    std::cout << "FlashKV CLI v1.0.0 (Connected to " << host << ":" << port << ")\n"
              << "Type 'help' or commands like 'SET mykey hello', 'GET mykey', 'QUIT'\n\n";

    std::string line;
    while (client.is_connected()) {
        std::cout << host << ":" << port << "> ";
        if (!std::getline(std::cin, line)) {
            break;
        }

        // Trim leading and trailing whitespace
        size_t first = line.find_first_not_of(" \t\r\n");
        if (first == std::string::npos) continue;
        size_t last = line.find_last_not_of(" \t\r\n");
        std::string trimmed = line.substr(first, (last - first + 1));

        if (trimmed == "quit" || trimmed == "exit" || trimmed == "QUIT") {
            break;
        }

        if (trimmed == "help" || trimmed == "HELP") {
            std::cout << "Commands supported:\n"
                      << "  Strings: SET key val [EX sec], GET key, DEL key, INCR key, DECR key, MGET, MSET\n"
                      << "  Hashes:  HSET key field val, HGET key field, HDEL, HGETALL, HLEN\n"
                      << "  Lists:   LPUSH key val, RPUSH key val, LPOP, RPOP, LLEN, LRANGE key start stop\n"
                      << "  Sets:    SADD key member, SREM, SMEMBERS, SISMEMBER, SCARD\n"
                      << "  Expiry:  EXPIRE key sec, TTL key, PERSIST key\n"
                      << "  Admin:   PING, ECHO, DBSIZE, INFO, FLUSHDB, SAVE, BGSAVE, BGREWRITEAOF\n";
            continue;
        }

        RespValue resp;
        std::string err;
        if (client.send_raw(trimmed, resp, err)) {
            print_resp_value(resp);
        } else {
            std::cerr << "Error: " << err << "\n";
            break;
        }
    }

    client.disconnect();
    SocketPlatform::cleanup();
    return 0;
}
