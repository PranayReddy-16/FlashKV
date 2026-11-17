#include <iostream>
#include <string>
#include <vector>
#include <algorithm>
#include <numeric>
#include <atomic>
#include <iomanip>
#include "net/socket.hpp"
#include "net/tcp_client.hpp"
#include "common/concurrency.hpp"
#include "common/time_utils.hpp"

using namespace flashkv;

struct BenchResult {
    std::string test_name;
    size_t total_requests;
    double elapsed_sec;
    double rps;
    double avg_latency_us;
    double p50_us;
    double p90_us;
    double p95_us;
    double p99_us;
    double p999_us;
    double min_us;
    double max_us;
};

void print_table(const std::vector<BenchResult>& results) {
    std::cout << "\n=======================================================================================================\n"
              << "                                     FLASHKV BENCHMARK RESULTS                                         \n"
              << "=======================================================================================================\n"
              << std::left << std::setw(12) << "Workload"
              << std::right << std::setw(10) << "Reqs"
              << std::setw(12) << "Time (s)"
              << std::setw(14) << "RPS (req/s)"
              << std::setw(12) << "Avg (us)"
              << std::setw(11) << "p50 (us)"
              << std::setw(11) << "p95 (us)"
              << std::setw(11) << "p99 (us)"
              << std::setw(11) << "Max (us)"
              << "\n"
              << "-------------------------------------------------------------------------------------------------------\n";

    for (const auto& r : results) {
        std::cout << std::left << std::setw(12) << r.test_name
                  << std::right << std::setw(10) << r.total_requests
                  << std::fixed << std::setprecision(3)
                  << std::setw(12) << r.elapsed_sec
                  << std::fixed << std::setprecision(1)
                  << std::setw(14) << r.rps
                  << std::fixed << std::setprecision(1)
                  << std::setw(12) << r.avg_latency_us
                  << std::setw(11) << r.p50_us
                  << std::setw(11) << r.p95_us
                  << std::setw(11) << r.p99_us
                  << std::setw(11) << r.max_us
                  << "\n";
    }
    std::cout << "=======================================================================================================\n\n";
}

BenchResult run_benchmark(const std::string& name,
                          const std::string& host,
                          int port,
                          size_t total_reqs,
                          size_t concurrency,
                          const std::string& payload_val) {
    size_t reqs_per_thread = total_reqs / concurrency;
    size_t actual_total = reqs_per_thread * concurrency;

    std::vector<Thread> threads;
    std::vector<std::vector<int64_t>> thread_latencies(concurrency);
    for (size_t i = 0; i < concurrency; ++i) {
        thread_latencies[i].reserve(reqs_per_thread);
    }

    std::atomic<bool> start_gate(false);

    int64_t overall_start_us = 0;

    for (size_t t = 0; t < concurrency; ++t) {
        threads.emplace_back([t, &host, port, reqs_per_thread, &name, &payload_val,
                              &start_gate, &thread_latencies]() {
            TcpClient client;
            if (!client.connect(host, port)) {
                std::cerr << "Thread " << t << " failed to connect!\n";
                return;
            }

            while (!start_gate.load()) {
                Thread::sleep_ms(1);
            }

            auto& latencies = thread_latencies[t];
            RespValue resp;
            std::string err;

            for (size_t i = 0; i < reqs_per_thread; ++i) {
                std::string key = "key:" + std::to_string(t) + ":" + std::to_string(i % 10000);
                int64_t t0 = TimeUtils::now_us();

                if (name == "SET") {
                    std::vector<std::string> cmd = {"SET", key, payload_val};
                    client.send_command(cmd, resp, err);
                } else if (name == "GET") {
                    std::vector<std::string> cmd = {"GET", key};
                    client.send_command(cmd, resp, err);
                } else if (name == "INCR") {
                    std::vector<std::string> cmd = {"INCR", "counter:" + std::to_string(t)};
                    client.send_command(cmd, resp, err);
                } else if (name == "MIXED") {
                    // 80% GET, 20% SET
                    if ((i % 5) == 0) {
                        std::vector<std::string> cmd = {"SET", key, payload_val};
                        client.send_command(cmd, resp, err);
                    } else {
                        std::vector<std::string> cmd = {"GET", key};
                        client.send_command(cmd, resp, err);
                    }
                } else if (name == "PING") {
                    std::vector<std::string> cmd = {"PING"};
                    client.send_command(cmd, resp, err);
                }

                int64_t t1 = TimeUtils::now_us();
                latencies.push_back(t1 - t0);
            }

            client.disconnect();
        });
    }

    // Give threads 50ms to establish all TCP sockets
    Thread::sleep_ms(50);
    overall_start_us = TimeUtils::now_us();
    start_gate.store(true);

    for (auto& th : threads) {
        th.join();
    }
    int64_t overall_end_us = TimeUtils::now_us();

    // Aggregate latencies
    std::vector<int64_t> all_latencies;
    all_latencies.reserve(actual_total);
    for (const auto& vec : thread_latencies) {
        all_latencies.insert(all_latencies.end(), vec.begin(), vec.end());
    }

    std::sort(all_latencies.begin(), all_latencies.end());

    double elapsed_sec = (overall_end_us - overall_start_us) / 1000000.0;
    double rps = actual_total / (elapsed_sec > 0 ? elapsed_sec : 0.001);

    double sum = 0;
    for (auto l : all_latencies) sum += l;
    double avg = all_latencies.empty() ? 0 : (sum / all_latencies.size());

    BenchResult br;
    br.test_name = name;
    br.total_requests = actual_total;
    br.elapsed_sec = elapsed_sec;
    br.rps = rps;
    br.avg_latency_us = avg;

    if (!all_latencies.empty()) {
        br.min_us = static_cast<double>(all_latencies.front());
        br.max_us = static_cast<double>(all_latencies.back());
        br.p50_us = static_cast<double>(all_latencies[all_latencies.size() * 50 / 100]);
        br.p90_us = static_cast<double>(all_latencies[all_latencies.size() * 90 / 100]);
        br.p95_us = static_cast<double>(all_latencies[all_latencies.size() * 95 / 100]);
        br.p99_us = static_cast<double>(all_latencies[all_latencies.size() * 99 / 100]);
        br.p999_us = static_cast<double>(all_latencies[all_latencies.size() * 999 / 1000]);
    }

    return br;
}

int main(int argc, char* argv[]) {
    SocketPlatform::init();

    std::string host = "127.0.0.1";
    int port = 6379;
    size_t total_requests = 50000;
    size_t concurrency = 20;
    size_t data_size = 32;
    std::string test_type = "ALL";

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "-h" && i + 1 < argc) host = argv[++i];
        else if (arg == "-p" && i + 1 < argc) port = std::atoi(argv[++i]);
        else if (arg == "-n" && i + 1 < argc) total_requests = static_cast<size_t>(std::atoi(argv[++i]));
        else if (arg == "-c" && i + 1 < argc) concurrency = static_cast<size_t>(std::atoi(argv[++i]));
        else if (arg == "-d" && i + 1 < argc) data_size = static_cast<size_t>(std::atoi(argv[++i]));
        else if (arg == "-t" && i + 1 < argc) test_type = argv[++i];
    }

    std::string payload_val(data_size, 'x');

    std::cout << "Starting FlashKV Benchmark...\n"
              << "Server:      " << host << ":" << port << "\n"
              << "Connections: " << concurrency << " parallel clients\n"
              << "Requests:    " << total_requests << " per benchmark\n"
              << "Data size:   " << data_size << " bytes\n";

    std::vector<BenchResult> results;

    if (test_type == "ALL" || test_type == "PING") {
        std::cout << ">>> Running PING benchmark...\n";
        results.push_back(run_benchmark("PING", host, port, total_requests, concurrency, payload_val));
    }
    if (test_type == "ALL" || test_type == "SET") {
        std::cout << ">>> Running SET benchmark...\n";
        results.push_back(run_benchmark("SET", host, port, total_requests, concurrency, payload_val));
    }
    if (test_type == "ALL" || test_type == "GET") {
        std::cout << ">>> Running GET benchmark...\n";
        results.push_back(run_benchmark("GET", host, port, total_requests, concurrency, payload_val));
    }
    if (test_type == "ALL" || test_type == "INCR") {
        std::cout << ">>> Running INCR benchmark...\n";
        results.push_back(run_benchmark("INCR", host, port, total_requests, concurrency, payload_val));
    }
    if (test_type == "ALL" || test_type == "MIXED") {
        std::cout << ">>> Running MIXED (80% Read / 20% Write) benchmark...\n";
        results.push_back(run_benchmark("MIXED", host, port, total_requests, concurrency, payload_val));
    }

    print_table(results);

    SocketPlatform::cleanup();
    return 0;
}
