#pragma once

#include <vector>
#include <queue>
#include <functional>
#include <atomic>
#include "common/concurrency.hpp"

namespace flashkv {

class ThreadPool {
public:
    explicit ThreadPool(size_t num_threads) : stop_(false), active_tasks_(0) {
        if (num_threads == 0) num_threads = 4;
        workers_.reserve(num_threads);
        for (size_t i = 0; i < num_threads; ++i) {
            workers_.emplace_back([this]() {
                this->worker_loop();
            });
        }
    }

    ~ThreadPool() {
        shutdown();
    }

    // Submit a task to the pool
    bool submit(std::function<void()> task) {
        {
            LockGuard lock(queue_mutex_);
            if (stop_) {
                return false;
            }
            tasks_.push(task);
        }
        cv_.notify_one();
        return true;
    }

    // Shutdown pool and wait for all workers to finish
    void shutdown() {
        {
            LockGuard lock(queue_mutex_);
            if (stop_) return;
            stop_ = true;
        }
        cv_.notify_all();
        for (auto& worker : workers_) {
            if (worker.joinable()) {
                worker.join();
            }
        }
        workers_.clear();
    }

    size_t worker_count() const {
        return workers_.size();
    }

    size_t pending_tasks() {
        LockGuard lock(queue_mutex_);
        return tasks_.size();
    }

    size_t active_tasks() const {
        return active_tasks_.load();
    }

private:
    void worker_loop() {
        while (true) {
            std::function<void()> task;
            {
                queue_mutex_.lock();
                while (!stop_ && tasks_.empty()) {
                    queue_mutex_.unlock();
                    Thread::sleep_ms(1); // Brief yield or condition wait
                    queue_mutex_.lock();
                }

                if (stop_ && tasks_.empty()) {
                    queue_mutex_.unlock();
                    break;
                }

                task = tasks_.front();
                tasks_.pop();
                queue_mutex_.unlock();
            }

            active_tasks_.fetch_add(1);
            try {
                task();
            } catch (...) {}
            active_tasks_.fetch_sub(1);
        }
    }

    std::vector<Thread> workers_;
    std::queue<std::function<void()>> tasks_;
    Mutex queue_mutex_;
    ConditionVariable cv_;
    bool stop_;
    std::atomic<size_t> active_tasks_;

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;
};

} // namespace flashkv
