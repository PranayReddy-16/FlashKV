#pragma once

#include <iostream>
#include <string>
#include <vector>
#include <memory>
#include <atomic>
#include <sstream>
#include <iomanip>
#include "server/server_config.hpp"
#include "net/socket.hpp"
#include "storage/engine.hpp"
#include "persistence/aof.hpp"
#include "persistence/rdb.hpp"
#include "protocol/command_parser.hpp"
#include "protocol/resp.hpp"
#include "common/concurrency.hpp"
#include "common/thread_pool.hpp"
#include "common/time_utils.hpp"

namespace flashkv {

class FlashServer {
public:
    explicit FlashServer(const ServerConfig& config)
        : config_(config),
          running_(false),
          engine_(config.num_shards),
          aof_(engine_, config.aof_path),
          thread_pool_(config.worker_threads),
          active_connections_(0),
          start_time_ms_(0) {
    }

    ~FlashServer() {
        stop();
    }

    bool start() {
        SocketPlatform::init();

        std::cout << "========================================================\n"
                  << "  ___ _           _    _  ___     __\n"
                  << " | __| |__ _ ____| |_ | |/ / |   / /  FlashKV in-memory store\n"
                  << " | _|| / _` (_-< ' \\ | ' < \\ \\ / /   v1.0.0 (High Performance)\n"
                  << " |_| |_\\__,_/__/_||_||_|\\_\\ \\_/_/    Port: " << config_.port << "\n"
                  << "========================================================\n"
                  << "[FlashKV] Initializing storage engine with " << config_.num_shards << " shards...\n";

        // Step 1: Restore persistence if available
        if (config_.enable_rdb) {
            size_t rdb_keys = RdbSnapshot::load(engine_, config_.rdb_path);
            if (rdb_keys > 0) {
                std::cout << "[FlashKV] Loaded " << rdb_keys << " keys from RDB snapshot: " << config_.rdb_path << "\n";
            }
        }

        if (config_.enable_aof) {
            size_t aof_cmds = aof_.replay();
            if (aof_cmds > 0) {
                std::cout << "[FlashKV] Replayed " << aof_cmds << " commands from AOF log: " << config_.aof_path << "\n";
            }
            if (!aof_.start()) {
                std::cerr << "[FlashKV] Failed to start AOF logger!\n";
            } else {
                std::cout << "[FlashKV] AOF persistence active (fsync policy: everysec)\n";
            }
        }

        // Step 2: Bind listener socket
        if (!listener_.bind_and_listen(config_.host, config_.port, 1024)) {
            std::cerr << "[FlashKV] Error: Failed to bind to " << config_.host << ":" << config_.port << "\n";
            return false;
        }

        running_ = true;
        start_time_ms_ = TimeUtils::now_ms();

        std::cout << "[FlashKV] TCP Server listening on " << config_.host << ":" << config_.port << "\n"
                  << "[FlashKV] Thread pool started with " << config_.worker_threads << " worker threads.\n"
                  << "[FlashKV] Ready to accept connections.\n";

        // Step 3: Start background maintenance thread (periodic active TTL eviction)
        maintenance_thread_ = Thread([this]() {
            this->maintenance_loop();
        });

        // Step 4: Acceptor loop
        accept_loop();

        return true;
    }

    void stop() {
        if (!running_) return;
        running_ = false;

        std::cout << "\n[FlashKV] Shutting down server gracefully...\n";

        listener_.close();

        if (maintenance_thread_.joinable()) {
            maintenance_thread_.join();
        }

        thread_pool_.shutdown();

        if (config_.enable_aof) {
            aof_.stop();
        }

        if (config_.enable_rdb) {
            std::cout << "[FlashKV] Saving final RDB snapshot to " << config_.rdb_path << "...\n";
            RdbSnapshot::save(engine_, config_.rdb_path);
        }

        SocketPlatform::cleanup();
        std::cout << "[FlashKV] Shutdown complete. Bye!\n";
    }

private:
    void accept_loop() {
        while (running_) {
            std::string client_ip;
            int client_port = 0;
            Socket client_sock = listener_.accept(&client_ip, &client_port);

            if (!client_sock.is_valid()) {
                if (!running_) break;
                Thread::sleep_ms(1);
                continue;
            }

            active_connections_.fetch_add(1);

            // Transfer client socket ownership into worker task
            socket_t raw_fd = client_sock.release();

            thread_pool_.submit([this, raw_fd, client_ip, client_port]() {
                Socket sock(raw_fd);
                this->handle_client(sock, client_ip, client_port);
                this->active_connections_.fetch_sub(1);
            });
        }
    }

    void handle_client(Socket& sock, const std::string& client_ip, int client_port) {
        if (config_.verbose) {
            std::cout << "[FlashKV] Client connected: " << client_ip << ":" << client_port << "\n";
        }

        std::string in_buf;
        char chunk[4096];

        while (running_) {
            int bytes = sock.recv_some(chunk, sizeof(chunk));
            if (bytes <= 0) {
                break; // Client disconnected or error
            }

            in_buf.append(chunk, static_cast<size_t>(bytes));

            while (!in_buf.empty()) {
                std::vector<std::string> args;
                std::string err_msg;
                auto status = CommandParser::parse(in_buf, args, err_msg);

                if (status == CommandParser::ParseStatus::SUCCESS) {
                    if (args.empty()) continue;

                    engine_.increment_commands();

                    // Check for QUIT command
                    if (args[0] == "QUIT") {
                        std::string resp = RespValue::make_ok().serialize();
                        sock.send_all(resp);
                        return;
                    }

                    // Execute command and serialize response
                    RespValue response = dispatch_command(args);
                    std::string out_wire = response.serialize();

                    if (sock.send_all(out_wire) < 0) {
                        return; // Send failure, client disconnected
                    }
                } else if (status == CommandParser::ParseStatus::INCOMPLETE) {
                    // Need more bytes from network
                    break;
                } else {
                    // Protocol error
                    std::string err_resp = RespValue::make_error("ERR " + err_msg).serialize();
                    sock.send_all(err_resp);
                    in_buf.clear();
                    break;
                }
            }
        }

        if (config_.verbose) {
            std::cout << "[FlashKV] Client disconnected: " << client_ip << ":" << client_port << "\n";
        }
    }

    RespValue dispatch_command(const std::vector<std::string>& args) {
        const std::string& cmd = args[0];

        // ----------------------------------------------------
        // Connection & Server Info
        // ----------------------------------------------------
        if (cmd == "PING") {
            if (args.size() == 1) {
                return RespValue::make_pong();
            } else {
                return RespValue::make_bulk_string(args[1]);
            }
        }

        if (cmd == "ECHO") {
            if (args.size() < 2) return RespValue::make_error("ERR wrong number of arguments for 'echo' command");
            return RespValue::make_bulk_string(args[1]);
        }

        if (cmd == "COMMAND") {
            // Needed by official redis-cli
            return RespValue::make_array({});
        }

        if (cmd == "INFO") {
            std::stringstream ss;
            int64_t uptime_sec = (TimeUtils::now_ms() - start_time_ms_) / 1000;
            ss << "# Server\r\n"
               << "flashkv_version:1.0.0\r\n"
               << "uptime_in_seconds:" << uptime_sec << "\r\n"
               << "shards:" << config_.num_shards << "\r\n"
               << "connected_clients:" << active_connections_.load() << "\r\n"
               << "total_commands_processed:" << engine_.total_commands() << "\r\n"
               << "total_reads:" << engine_.total_reads() << "\r\n"
               << "total_writes:" << engine_.total_writes() << "\r\n"
               << "dbsize:" << engine_.dbsize() << "\r\n";
            return RespValue::make_bulk_string(ss.str());
        }

        if (cmd == "DBSIZE") {
            return RespValue::make_integer(static_cast<int64_t>(engine_.dbsize()));
        }

        if (cmd == "FLUSHDB" || cmd == "FLUSHALL") {
            engine_.flushdb();
            if (config_.enable_aof) aof_.append_command(args);
            return RespValue::make_ok();
        }

        if (cmd == "SAVE") {
            bool ok = RdbSnapshot::save(engine_, config_.rdb_path);
            return ok ? RespValue::make_ok() : RespValue::make_error("ERR failed to save RDB");
        }

        if (cmd == "BGSAVE") {
            thread_pool_.submit([this]() {
                RdbSnapshot::save(this->engine_, this->config_.rdb_path);
            });
            return RespValue::make_simple_string("Background saving started");
        }

        if (cmd == "BGREWRITEAOF") {
            thread_pool_.submit([this]() {
                this->aof_.rewrite_aof();
            });
            return RespValue::make_simple_string("Background append only file rewriting started");
        }

        // ----------------------------------------------------
        // String Commands
        // ----------------------------------------------------
        if (cmd == "SET") {
            if (args.size() < 3) return RespValue::make_error("ERR wrong number of arguments for 'set' command");
            int64_t expire_at = 0;

            if (args.size() >= 5) {
                std::string opt = CommandParser::to_upper(args[3]);
                if (opt == "EX") {
                    int64_t sec = std::strtoll(args[4].c_str(), nullptr, 10);
                    if (sec <= 0) return RespValue::make_error("ERR invalid expire time in 'set' command");
                    expire_at = TimeUtils::now_ms() + sec * 1000;
                } else if (opt == "PX") {
                    int64_t ms = std::strtoll(args[4].c_str(), nullptr, 10);
                    if (ms <= 0) return RespValue::make_error("ERR invalid expire time in 'set' command");
                    expire_at = TimeUtils::now_ms() + ms;
                }
            }

            engine_.set(args[1], args[2], expire_at);
            if (config_.enable_aof) aof_.append_command(args);
            return RespValue::make_ok();
        }

        if (cmd == "SETEX") {
            if (args.size() < 4) return RespValue::make_error("ERR wrong number of arguments for 'setex' command");
            int64_t sec = std::strtoll(args[2].c_str(), nullptr, 10);
            if (sec <= 0) return RespValue::make_error("ERR invalid expire time in 'setex' command");
            int64_t expire_at = TimeUtils::now_ms() + sec * 1000;
            engine_.set(args[1], args[3], expire_at);
            if (config_.enable_aof) aof_.append_command(args);
            return RespValue::make_ok();
        }

        if (cmd == "GET") {
            if (args.size() != 2) return RespValue::make_error("ERR wrong number of arguments for 'get' command");
            std::string val;
            if (engine_.get(args[1], val)) {
                return RespValue::make_bulk_string(val);
            }
            return RespValue::make_null_bulk_string();
        }

        if (cmd == "DEL") {
            if (args.size() < 2) return RespValue::make_error("ERR wrong number of arguments for 'del' command");
            int64_t count = 0;
            for (size_t i = 1; i < args.size(); ++i) {
                if (engine_.del(args[i])) count++;
            }
            if (count > 0 && config_.enable_aof) aof_.append_command(args);
            return RespValue::make_integer(count);
        }

        if (cmd == "EXISTS") {
            if (args.size() < 2) return RespValue::make_error("ERR wrong number of arguments for 'exists' command");
            int64_t count = 0;
            for (size_t i = 1; i < args.size(); ++i) {
                if (engine_.exists(args[i])) count++;
            }
            return RespValue::make_integer(count);
        }

        if (cmd == "TYPE") {
            if (args.size() != 2) return RespValue::make_error("ERR wrong number of arguments for 'type' command");
            DataType dt = engine_.get_type(args[1]);
            return RespValue::make_simple_string(data_type_to_string(dt));
        }

        if (cmd == "INCR") {
            if (args.size() != 2) return RespValue::make_error("ERR wrong number of arguments for 'incr' command");
            int64_t res = 0;
            std::string err;
            if (engine_.incr_by(args[1], 1, res, err)) {
                if (config_.enable_aof) aof_.append_command(args);
                return RespValue::make_integer(res);
            }
            return RespValue::make_error(err);
        }

        if (cmd == "DECR") {
            if (args.size() != 2) return RespValue::make_error("ERR wrong number of arguments for 'decr' command");
            int64_t res = 0;
            std::string err;
            if (engine_.incr_by(args[1], -1, res, err)) {
                if (config_.enable_aof) aof_.append_command(args);
                return RespValue::make_integer(res);
            }
            return RespValue::make_error(err);
        }

        if (cmd == "INCRBY") {
            if (args.size() != 3) return RespValue::make_error("ERR wrong number of arguments for 'incrby' command");
            int64_t delta = std::strtoll(args[2].c_str(), nullptr, 10);
            int64_t res = 0;
            std::string err;
            if (engine_.incr_by(args[1], delta, res, err)) {
                if (config_.enable_aof) aof_.append_command(args);
                return RespValue::make_integer(res);
            }
            return RespValue::make_error(err);
        }

        if (cmd == "MGET") {
            if (args.size() < 2) return RespValue::make_error("ERR wrong number of arguments for 'mget' command");
            std::vector<RespValue> elements;
            elements.reserve(args.size() - 1);
            for (size_t i = 1; i < args.size(); ++i) {
                std::string val;
                if (engine_.get(args[i], val)) {
                    elements.push_back(RespValue::make_bulk_string(val));
                } else {
                    elements.push_back(RespValue::make_null_bulk_string());
                }
            }
            return RespValue::make_array(elements);
        }

        if (cmd == "MSET") {
            if (args.size() < 3 || (args.size() % 2 == 0)) {
                return RespValue::make_error("ERR wrong number of arguments for 'mset' command");
            }
            for (size_t i = 1; i < args.size(); i += 2) {
                engine_.set(args[i], args[i + 1]);
            }
            if (config_.enable_aof) aof_.append_command(args);
            return RespValue::make_ok();
        }

        // ----------------------------------------------------
        // Hash Commands
        // ----------------------------------------------------
        if (cmd == "HSET") {
            if (args.size() < 4) return RespValue::make_error("ERR wrong number of arguments for 'hset' command");
            int res = engine_.hset(args[1], args[2], args[3]);
            if (res < 0) return RespValue::make_error("WRONGTYPE Operation against a key holding the wrong kind of value");
            if (config_.enable_aof) aof_.append_command(args);
            return RespValue::make_integer(res);
        }

        if (cmd == "HGET") {
            if (args.size() != 3) return RespValue::make_error("ERR wrong number of arguments for 'hget' command");
            std::string val;
            int res = engine_.hget(args[1], args[2], val);
            if (res < 0) return RespValue::make_error("WRONGTYPE Operation against a key holding the wrong kind of value");
            if (res == 1) return RespValue::make_bulk_string(val);
            return RespValue::make_null_bulk_string();
        }

        if (cmd == "HDEL") {
            if (args.size() < 3) return RespValue::make_error("ERR wrong number of arguments for 'hdel' command");
            std::vector<std::string> fields(args.begin() + 2, args.end());
            int res = engine_.hdel(args[1], fields);
            if (res < 0) return RespValue::make_error("WRONGTYPE Operation against a key holding the wrong kind of value");
            if (res > 0 && config_.enable_aof) aof_.append_command(args);
            return RespValue::make_integer(res);
        }

        if (cmd == "HLEN") {
            if (args.size() != 2) return RespValue::make_error("ERR wrong number of arguments for 'hlen' command");
            int64_t res = engine_.hlen(args[1]);
            if (res < 0) return RespValue::make_error("WRONGTYPE Operation against a key holding the wrong kind of value");
            return RespValue::make_integer(res);
        }

        if (cmd == "HGETALL") {
            if (args.size() != 2) return RespValue::make_error("ERR wrong number of arguments for 'hgetall' command");
            std::vector<std::pair<std::string, std::string>> pairs;
            int res = engine_.hgetall(args[1], pairs);
            if (res < 0) return RespValue::make_error("WRONGTYPE Operation against a key holding the wrong kind of value");

            std::vector<RespValue> elements;
            elements.reserve(pairs.size() * 2);
            for (const auto& kv : pairs) {
                elements.push_back(RespValue::make_bulk_string(kv.first));
                elements.push_back(RespValue::make_bulk_string(kv.second));
            }
            return RespValue::make_array(elements);
        }

        // ----------------------------------------------------
        // List Commands
        // ----------------------------------------------------
        if (cmd == "LPUSH") {
            if (args.size() < 3) return RespValue::make_error("ERR wrong number of arguments for 'lpush' command");
            std::vector<std::string> vals(args.begin() + 2, args.end());
            int64_t res = engine_.lpush(args[1], vals);
            if (res < 0) return RespValue::make_error("WRONGTYPE Operation against a key holding the wrong kind of value");
            if (config_.enable_aof) aof_.append_command(args);
            return RespValue::make_integer(res);
        }

        if (cmd == "RPUSH") {
            if (args.size() < 3) return RespValue::make_error("ERR wrong number of arguments for 'rpush' command");
            std::vector<std::string> vals(args.begin() + 2, args.end());
            int64_t res = engine_.rpush(args[1], vals);
            if (res < 0) return RespValue::make_error("WRONGTYPE Operation against a key holding the wrong kind of value");
            if (config_.enable_aof) aof_.append_command(args);
            return RespValue::make_integer(res);
        }

        if (cmd == "LPOP") {
            if (args.size() != 2) return RespValue::make_error("ERR wrong number of arguments for 'lpop' command");
            std::string val;
            int res = engine_.lpop(args[1], val);
            if (res < 0) return RespValue::make_error("WRONGTYPE Operation against a key holding the wrong kind of value");
            if (res == 1) {
                if (config_.enable_aof) aof_.append_command(args);
                return RespValue::make_bulk_string(val);
            }
            return RespValue::make_null_bulk_string();
        }

        if (cmd == "RPOP") {
            if (args.size() != 2) return RespValue::make_error("ERR wrong number of arguments for 'rpop' command");
            std::string val;
            int res = engine_.rpop(args[1], val);
            if (res < 0) return RespValue::make_error("WRONGTYPE Operation against a key holding the wrong kind of value");
            if (res == 1) {
                if (config_.enable_aof) aof_.append_command(args);
                return RespValue::make_bulk_string(val);
            }
            return RespValue::make_null_bulk_string();
        }

        if (cmd == "LLEN") {
            if (args.size() != 2) return RespValue::make_error("ERR wrong number of arguments for 'llen' command");
            int64_t res = engine_.llen(args[1]);
            if (res < 0) return RespValue::make_error("WRONGTYPE Operation against a key holding the wrong kind of value");
            return RespValue::make_integer(res);
        }

        if (cmd == "LRANGE") {
            if (args.size() != 4) return RespValue::make_error("ERR wrong number of arguments for 'lrange' command");
            int64_t start = std::strtoll(args[2].c_str(), nullptr, 10);
            int64_t stop = std::strtoll(args[3].c_str(), nullptr, 10);
            std::vector<std::string> elems;
            int res = engine_.lrange(args[1], start, stop, elems);
            if (res < 0) return RespValue::make_error("WRONGTYPE Operation against a key holding the wrong kind of value");

            std::vector<RespValue> elements;
            elements.reserve(elems.size());
            for (const auto& item : elems) {
                elements.push_back(RespValue::make_bulk_string(item));
            }
            return RespValue::make_array(elements);
        }

        // ----------------------------------------------------
        // Set Commands
        // ----------------------------------------------------
        if (cmd == "SADD") {
            if (args.size() < 3) return RespValue::make_error("ERR wrong number of arguments for 'sadd' command");
            std::vector<std::string> members(args.begin() + 2, args.end());
            int64_t res = engine_.sadd(args[1], members);
            if (res < 0) return RespValue::make_error("WRONGTYPE Operation against a key holding the wrong kind of value");
            if (res > 0 && config_.enable_aof) aof_.append_command(args);
            return RespValue::make_integer(res);
        }

        if (cmd == "SREM") {
            if (args.size() < 3) return RespValue::make_error("ERR wrong number of arguments for 'srem' command");
            std::vector<std::string> members(args.begin() + 2, args.end());
            int64_t res = engine_.srem(args[1], members);
            if (res < 0) return RespValue::make_error("WRONGTYPE Operation against a key holding the wrong kind of value");
            if (res > 0 && config_.enable_aof) aof_.append_command(args);
            return RespValue::make_integer(res);
        }

        if (cmd == "SMEMBERS") {
            if (args.size() != 2) return RespValue::make_error("ERR wrong number of arguments for 'smembers' command");
            std::vector<std::string> members;
            int res = engine_.smembers(args[1], members);
            if (res < 0) return RespValue::make_error("WRONGTYPE Operation against a key holding the wrong kind of value");

            std::vector<RespValue> elements;
            elements.reserve(members.size());
            for (const auto& m : members) {
                elements.push_back(RespValue::make_bulk_string(m));
            }
            return RespValue::make_array(elements);
        }

        if (cmd == "SISMEMBER") {
            if (args.size() != 3) return RespValue::make_error("ERR wrong number of arguments for 'sismember' command");
            int res = engine_.sismember(args[1], args[2]);
            if (res < 0) return RespValue::make_error("WRONGTYPE Operation against a key holding the wrong kind of value");
            return RespValue::make_integer(res);
        }

        if (cmd == "SCARD") {
            if (args.size() != 2) return RespValue::make_error("ERR wrong number of arguments for 'scard' command");
            int64_t res = engine_.scard(args[1]);
            if (res < 0) return RespValue::make_error("WRONGTYPE Operation against a key holding the wrong kind of value");
            return RespValue::make_integer(res);
        }

        // ----------------------------------------------------
        // TTL / Expiration Commands
        // ----------------------------------------------------
        if (cmd == "EXPIRE") {
            if (args.size() != 3) return RespValue::make_error("ERR wrong number of arguments for 'expire' command");
            int64_t sec = std::strtoll(args[2].c_str(), nullptr, 10);
            bool ok = engine_.expire(args[1], sec);
            if (ok && config_.enable_aof) aof_.append_command(args);
            return RespValue::make_integer(ok ? 1 : 0);
        }

        if (cmd == "TTL") {
            if (args.size() != 2) return RespValue::make_error("ERR wrong number of arguments for 'ttl' command");
            int64_t remaining = engine_.ttl(args[1]);
            return RespValue::make_integer(remaining);
        }

        if (cmd == "PERSIST") {
            if (args.size() != 2) return RespValue::make_error("ERR wrong number of arguments for 'persist' command");
            bool ok = engine_.persist(args[1]);
            if (ok && config_.enable_aof) aof_.append_command(args);
            return RespValue::make_integer(ok ? 1 : 0);
        }

        // Unknown command
        return RespValue::make_error("ERR unknown command '" + cmd + "'");
    }

    void maintenance_loop() {
        while (running_) {
            Thread::sleep_ms(100); // 10 Hz cycle like Redis activeExpireCycle
            if (!running_) break;
            engine_.run_active_expiry_cycle(16, 20);
        }
    }

    ServerConfig config_;
    bool running_;
    StorageEngine engine_;
    AofManager aof_;
    ThreadPool thread_pool_;
    Socket listener_;
    Thread maintenance_thread_;
    std::atomic<int> active_connections_;
    int64_t start_time_ms_;
};

} // namespace flashkv
