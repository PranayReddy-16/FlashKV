#include <iostream>
#include <cassert>
#include <string>
#include <vector>
#include <atomic>
#include "protocol/resp.hpp"
#include "protocol/command_parser.hpp"
#include "storage/engine.hpp"
#include "persistence/aof.hpp"
#include "persistence/rdb.hpp"
#include "common/concurrency.hpp"
#include "common/thread_pool.hpp"

using namespace flashkv;

#define TEST_ASSERT(cond, msg) \
    do { \
        if (!(cond)) { \
            std::cerr << "FAILED: " << msg << " (" << __FILE__ << ":" << __LINE__ << ")\n"; \
            std::exit(1); \
        } \
    } while(0)

void test_resp_serialization() {
    std::cout << "[Test] Running RESP serialization tests...\n";

    // Simple string
    RespValue s = RespValue::make_simple_string("OK");
    TEST_ASSERT(s.serialize() == "+OK\r\n", "Simple string serialization");

    // Error
    RespValue err = RespValue::make_error("Error message");
    TEST_ASSERT(err.serialize() == "-Error message\r\n", "Error serialization");

    // Integer
    RespValue i = RespValue::make_integer(100);
    TEST_ASSERT(i.serialize() == ":100\r\n", "Integer serialization");

    // Bulk string
    RespValue b = RespValue::make_bulk_string("foobar");
    TEST_ASSERT(b.serialize() == "$6\r\nfoobar\r\n", "Bulk string serialization");

    // Null bulk string
    RespValue nb = RespValue::make_null_bulk_string();
    TEST_ASSERT(nb.serialize() == "$-1\r\n", "Null bulk string serialization");

    // Array
    RespValue arr = RespValue::make_array({
        RespValue::make_bulk_string("SET"),
        RespValue::make_bulk_string("key"),
        RespValue::make_bulk_string("val")
    });
    TEST_ASSERT(arr.serialize() == "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$3\r\nval\r\n", "Array serialization");

    std::cout << "  -> RESP serialization tests passed!\n";
}

void test_command_parser() {
    std::cout << "[Test] Running command parser tests...\n";

    // Test 1: Full RESP Array
    std::string buf = "*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$5\r\nalice\r\n";
    std::vector<std::string> args;
    std::string err;
    auto status = CommandParser::parse(buf, args, err);
    TEST_ASSERT(status == CommandParser::ParseStatus::SUCCESS, "RESP parse success");
    TEST_ASSERT(args.size() == 3, "RESP args count");
    TEST_ASSERT(args[0] == "SET", "Command name uppercase");
    TEST_ASSERT(args[1] == "name", "Arg 1");
    TEST_ASSERT(args[2] == "alice", "Arg 2");
    TEST_ASSERT(buf.empty(), "Buffer consumed");

    // Test 2: Incomplete RESP buffer
    std::string partial = "*3\r\n$3\r\nSET\r\n$4\r\nna";
    args.clear();
    status = CommandParser::parse(partial, args, err);
    TEST_ASSERT(status == CommandParser::ParseStatus::INCOMPLETE, "Partial buffer handled cleanly");
    TEST_ASSERT(partial == "*3\r\n$3\r\nSET\r\n$4\r\nna", "Partial buffer untouched");

    // Test 3: Inline Command with spaces and quotes
    std::string inline_cmd = "set mykey \"hello world\"\r\n";
    args.clear();
    status = CommandParser::parse(inline_cmd, args, err);
    TEST_ASSERT(status == CommandParser::ParseStatus::SUCCESS, "Inline parse success");
    TEST_ASSERT(args.size() == 3, "Inline args count");
    TEST_ASSERT(args[0] == "SET", "Inline command uppercase");
    TEST_ASSERT(args[1] == "mykey", "Inline key");
    TEST_ASSERT(args[2] == "hello world", "Inline quoted val");

    std::cout << "  -> Command parser tests passed!\n";
}

void test_storage_engine_crud() {
    std::cout << "[Test] Running storage engine CRUD tests...\n";
    StorageEngine engine(16);

    // String operations
    TEST_ASSERT(engine.set("foo", "bar"), "SET foo bar");
    std::string val;
    TEST_ASSERT(engine.get("foo", val) && val == "bar", "GET foo");
    TEST_ASSERT(engine.exists("foo"), "EXISTS foo");
    TEST_ASSERT(engine.get_type("foo") == DataType::STRING, "TYPE foo is STRING");

    // INCR / DECR
    int64_t counter = 0;
    std::string err;
    TEST_ASSERT(engine.incr_by("counter", 1, counter, err) && counter == 1, "INCR counter");
    TEST_ASSERT(engine.incr_by("counter", 9, counter, err) && counter == 10, "INCRBY counter 9");
    TEST_ASSERT(engine.incr_by("counter", -3, counter, err) && counter == 7, "DECRBY counter 3");

    // Hash operations
    TEST_ASSERT(engine.hset("user:1", "name", "Bob") == 1, "HSET user:1 name Bob");
    TEST_ASSERT(engine.hset("user:1", "city", "NYC") == 1, "HSET user:1 city NYC");
    TEST_ASSERT(engine.hlen("user:1") == 2, "HLEN user:1");
    std::string hval;
    TEST_ASSERT(engine.hget("user:1", "city", hval) == 1 && hval == "NYC", "HGET user:1 city");
    std::vector<std::pair<std::string, std::string>> all_hash;
    TEST_ASSERT(engine.hgetall("user:1", all_hash) == 1 && all_hash.size() == 2, "HGETALL user:1");

    // List operations
    std::vector<std::string> items = {"apple", "banana"};
    TEST_ASSERT(engine.rpush("fruits", items) == 2, "RPUSH fruits");
    std::vector<std::string> litems = {"cherry"};
    TEST_ASSERT(engine.lpush("fruits", litems) == 3, "LPUSH fruits cherry");
    TEST_ASSERT(engine.llen("fruits") == 3, "LLEN fruits");

    std::string pop_val;
    TEST_ASSERT(engine.lpop("fruits", pop_val) == 1 && pop_val == "cherry", "LPOP fruits");
    TEST_ASSERT(engine.rpop("fruits", pop_val) == 1 && pop_val == "banana", "RPOP fruits");

    // Set operations
    std::vector<std::string> set_members = {"m1", "m2", "m3", "m1"};
    TEST_ASSERT(engine.sadd("myset", set_members) == 3, "SADD myset");
    TEST_ASSERT(engine.scard("myset") == 3, "SCARD myset");
    TEST_ASSERT(engine.sismember("myset", "m2") == 1, "SISMEMBER myset m2");
    TEST_ASSERT(engine.sismember("myset", "m99") == 0, "SISMEMBER myset m99");

    // DEL
    TEST_ASSERT(engine.del("foo"), "DEL foo");
    TEST_ASSERT(!engine.exists("foo"), "foo is deleted");

    std::cout << "  -> Storage engine CRUD tests passed!\n";
}

void test_ttl_expiration() {
    std::cout << "[Test] Running TTL and expiration tests...\n";
    StorageEngine engine(16);

    // Set key with 1 second TTL
    int64_t expire_at = TimeUtils::now_ms() + 1000;
    engine.set("temp_key", "temporary", expire_at);

    TEST_ASSERT(engine.exists("temp_key"), "temp_key exists immediately");
    TEST_ASSERT(engine.ttl("temp_key") >= 1, "TTL is at least 1s");

    // Sleep for 1100ms
    Thread::sleep_ms(1100);

    // Lazy expiration test
    std::string val;
    TEST_ASSERT(!engine.get("temp_key", val), "temp_key lazily expired on GET");
    TEST_ASSERT(!engine.exists("temp_key"), "temp_key does not exist after expiry");

    std::cout << "  -> TTL and expiration tests passed!\n";
}

void test_multithreaded_concurrency() {
    std::cout << "[Test] Running multithreaded concurrency tests (16 threads, 50,000 ops)...\n";
    StorageEngine engine(32);
    const size_t num_threads = 16;
    const size_t ops_per_thread = 3000;

    std::vector<Thread> threads;
    std::atomic<bool> start_signal(false);

    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([t, &engine, ops_per_thread, &start_signal]() {
            while (!start_signal.load()) {
                Thread::sleep_ms(1);
            }

            for (size_t i = 0; i < ops_per_thread; ++i) {
                std::string key = "c_key:" + std::to_string(t) + ":" + std::to_string(i % 500);
                engine.set(key, "data_" + std::to_string(i));

                std::string read_val;
                engine.get(key, read_val);

                int64_t new_val = 0;
                std::string err;
                engine.incr_by("shared_counter", 1, new_val, err);
            }
        });
    }

    start_signal.store(true);

    for (auto& th : threads) {
        th.join();
    }

    int64_t final_counter = 0;
    std::string err;
    engine.incr_by("shared_counter", 0, final_counter, err);
    TEST_ASSERT(final_counter == static_cast<int64_t>(num_threads * ops_per_thread), "Atomic counter matches ops");

    std::cout << "  -> Multithreaded concurrency tests passed! (Atomic counter: " << final_counter << ")\n";
}

void test_persistence() {
    std::cout << "[Test] Running RDB snapshot and AOF persistence tests...\n";

    // 1. RDB Test
    {
        StorageEngine engine(16);
        engine.set("k1", "hello");
        engine.set("k2", "world");
        engine.hset("hash1", "fieldA", "valA");

        TEST_ASSERT(RdbSnapshot::save(engine, "test_dump.rdb"), "RDB save");

        StorageEngine engine2(16);
        size_t loaded = RdbSnapshot::load(engine2, "test_dump.rdb");
        TEST_ASSERT(loaded == 3, "Loaded 3 keys from RDB");

        std::string v1, v2, hv;
        TEST_ASSERT(engine2.get("k1", v1) && v1 == "hello", "Restored k1");
        TEST_ASSERT(engine2.get("k2", v2) && v2 == "world", "Restored k2");
        TEST_ASSERT(engine2.hget("hash1", "fieldA", hv) == 1 && hv == "valA", "Restored hash field");

        std::remove("test_dump.rdb");
    }

    // 2. AOF Replay Test
    {
        StorageEngine engine(16);
        AofManager aof(engine, "test_append.aof", AofSyncPolicy::ALWAYS);
        aof.start();

        aof.append_command({"SET", "aof_k1", "aof_v1"});
        aof.append_command({"SET", "aof_k2", "100"});
        aof.append_command({"INCRBY", "aof_k2", "50"});
        aof.append_command({"HSET", "aof_h1", "f1", "v1"});

        aof.stop();

        StorageEngine restored_engine(16);
        AofManager replay_aof(restored_engine, "test_append.aof");
        size_t replayed = replay_aof.replay();
        TEST_ASSERT(replayed >= 3, "Replayed AOF commands");

        std::string v1, v2;
        TEST_ASSERT(restored_engine.get("aof_k1", v1) && v1 == "aof_v1", "AOF restored k1");
        TEST_ASSERT(restored_engine.get("aof_k2", v2) && v2 == "150", "AOF restored k2 with INCRBY");

        std::remove("test_append.aof");
    }

    std::cout << "  -> Persistence tests passed!\n";
}

int main() {
    std::cout << "===========================================\n"
              << "       FLASHKV UNIT & INTEGRATION TESTS    \n"
              << "===========================================\n";

    test_resp_serialization();
    test_command_parser();
    test_storage_engine_crud();
    test_ttl_expiration();
    test_multithreaded_concurrency();
    test_persistence();

    std::cout << "\n>>> ALL TESTS PASSED SUCCESSFULLY! <<<\n";
    return 0;
}
