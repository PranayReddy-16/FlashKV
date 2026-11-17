# FlashKV: High-Performance Architecture & Systems Design

FlashKV is a production-grade, in-memory key-value store engineered from scratch in modern C++. It provides a full Redis-compatible TCP server, an extensible command parser implementing the REdis Serialization Protocol (RESP v2), a striped concurrent storage engine, dual disk persistence mechanisms (AOF & RDB), active/lazy TTL expiration, an interactive CLI, and a multi-threaded benchmarking suite.

---

## 1. High-Level Architecture Diagram

```
 +-----------------------------------------------------------------------+
 |                            TCP Clients                                |
 |               (redis-cli, flashkv-cli, flashkv-bench, SDKs)           |
 +-----------------------------------+-----------------------------------+
                                     |  TCP Sockets (RESP v2 / Inline)
                                     v
 +-----------------------------------------------------------------------+
 |                     Acceptor & Thread Pool Worker                     |
 |    - Winsock2 / POSIX TCP Socket Layer (TCP_NODELAY, SO_REUSEADDR)   |
 |    - Non-blocking I/O & Connection Task Dispatch                      |
 +-----------------------------------+-----------------------------------+
                                     |  Stream Byte Chunks
                                     v
 +-----------------------------------------------------------------------+
 |                    Command Parser & RESP Engine                       |
 |    - Framing Parser: Arrays, Bulk Strings, Integers, Errors, Inline   |
 |    - Case-insensitive tokenization with quoted parameter support      |
 +-----------------------------------+-----------------------------------+
                                     |  Command Dispatched
                                     v
 +-----------------------------------------------------------------------+
 |                   Striped In-Memory Storage Engine                    |
 |                                                                       |
 |   +-------------+  +-------------+  +-------------+  +-------------+  |
 |   |   Shard 0   |  |   Shard 1   |  |     ...     |  |  Shard 63   |  |
 |   | SharedMutex |  | SharedMutex |  | SharedMutex |  | SharedMutex |  |
 |   |  (RW-Lock)  |  |  (RW-Lock)  |  |  (RW-Lock)  |  |  (RW-Lock)  |  |
 |   |  HashTable  |  |  HashTable  |  |  HashTable  |  |  HashTable  |  |
 |   +-------------+  +-------------+  +-------------+  +-------------+  |
 |           ^                                                   ^       |
 |           +----------------- FNV-1a Hashing ------------------+       |
 +-----------------------------------+-----------------------------------+
                                     |
               +---------------------+---------------------+
               | Mutating Writes                           | Snapshots
               v                                           v
 +-----------------------------+             +---------------------------+
 |   Append-Only File (AOF)    |             |   RDB Snapshot Engine     |
 | - Sequential log append     |             | - Compact binary dump     |
 | - Sync policies (everysec)  |             | - Magic header + checksum |
 | - In-memory AOF rewrite     |             | - Fast boot restoration   |
 +-----------------------------+             +---------------------------+
```

---

## 2. Striped Hash Partitioning & Concurrency

### The Single-Lock Bottleneck
In naive key-value stores, a single global mutex protects the entire database map:
```
[Client 1] -----\
[Client 2] -----> [ Single Global Mutex ] ----> [ std::unordered_map ]
[Client 3] -----/
```
Under high concurrency (e.g., 16-64 threads), threads spend most of their CPU cycles waiting in thread queues for lock acquisition, cache line bouncing (cache coherency traffic across L1/L2 caches), and context switching.

### FlashKV Striped Locking
FlashKV partitions its key space across $N = 64$ independent shards:
$$\text{shard\_index} = \text{FNV1a}(\text{key}) \ \& \ (N - 1)$$

Each shard maintains its own independent `SharedMutex` (Reader-Writer Lock):
- **Reads** (`GET`, `MGET`, `EXISTS`, `TYPE`, `HGET`, `LLEN`, `SMEMBERS`, `TTL`):
  Acquire a **shared reader lock** (`AcquireSRWLockShared`). Multiple threads can read keys in the same shard simultaneously without blocking.
- **Writes** (`SET`, `DEL`, `INCR`, `HSET`, `LPUSH`, `SADD`):
  Acquire an **exclusive writer lock** (`AcquireSRWLockExclusive`) solely on that individual shard.

Concurrent operations on different keys map to different shards and run with **zero lock contention**.

---

## 3. Custom RAII Concurrency Primitives

FlashKV provides a platform abstraction layer in `include/common/concurrency.hpp`:
- `Mutex` & `LockGuard`: Lightweight mutual exclusion wrapping `CRITICAL_SECTION` on Windows and `pthread_mutex_t` on POSIX.
- `SharedMutex`, `SharedLock`, and `UniqueLock`: Reader-Writer lock wrapping Windows Slim Reader/Writer (`SRWLOCK`) and POSIX `pthread_rwlock_t`.
- `ConditionVariable`: High-efficiency wait/notify mechanism wrapping `CONDITION_VARIABLE` and `pthread_cond_t`.
- `Thread`: Object-oriented RAII thread wrapper around `_beginthreadex` and `pthread_create`, supporting lambdas, parameter binding, joining, and detaching.
- `ThreadPool`: Thread-safe work queue with condition-variable-based sleep/wake mechanics, avoiding CPU busy-waiting.

---

## 4. REdis Serialization Protocol (RESP v2)

FlashKV communicates using standard Redis wire protocol:
- **Simple Strings**: `+<string>\r\n` (e.g., `+OK\r\n`)
- **Errors**: `-<message>\r\n` (e.g., `-ERR unknown command\r\n`)
- **Integers**: `:<number>\r\n` (e.g., `:100\r\n`)
- **Bulk Strings**: `$<len>\r\n<payload>\r\n` or `$-1\r\n` for null values
- **Arrays**: `*<count>\r\n<element_1>...<element_n>\r\n`

### Streaming Buffer & Framing
Network TCP packets can arrive fragmented. FlashKV's `CommandParser::parse` maintains an input accumulator buffer per connection:
- If a complete RESP frame is present: extracts arguments, consumes exact bytes from buffer, and returns `ParseStatus::SUCCESS`.
- If incomplete: leaves buffer intact and returns `ParseStatus::INCOMPLETE` to await subsequent socket packets.
- Also supports **Inline commands** (e.g., typing `SET key val\r\n` directly into telnet or netcat).

---

## 5. Dual Disk Persistence Engine

### 1. Append-Only File (AOF)
- Appends every state-mutating command (`SET`, `DEL`, `INCR`, `HSET`, etc.) to disk in standard RESP array format.
- **Durability Policies**:
  - `ALWAYS`: Flushes and calls `_commit`/`fsync` after each write.
  - `EVERYSEC`: Background flusher thread issues an `fsync` once per second, offering high throughput with at most 1 second of data loss on power failure.
  - `NO`: Relies on OS filesystem page cache write-back.
- **AOF Replay**: On startup, FlashKV reads `appendonly.aof` through the `CommandParser` to reconstruct database state.
- **Compaction / AOF Rewrite (`BGREWRITEAOF`)**: Reads in-memory state and outputs a condensed command history (e.g., writing only the latest state of each key with remaining TTL), keeping file size strictly bounded.

### 2. RDB Binary Snapshotting
- Structured, compact snapshot format:
  `[MAGIC 7B: FLASHKV] [VERSION 1B] [KEY_COUNT 8B] [RECORDS...]`
- Each record encodes data type, expiry timestamp, key, and type-specific data payload.
- Can be triggered synchronously via `SAVE` or asynchronously in a worker thread via `BGSAVE`.

---

## 6. Two-Tier TTL Expiration

1. **Lazy (Passive) Expiration**:
   Whenever a key is requested (`get`, `exists`, `ttl`, etc.), its expiration timestamp is checked against `TimeUtils::now_ms()`. If expired, it is deleted and treated as non-existent with $O(1)$ overhead.
2. **Active (Periodic) Expiration**:
   A dedicated maintenance thread wakes at 10 Hz (every 100ms), samples random shards and purges expired keys. This prevents memory leaks from keys that are written with a TTL but never read again.
