#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <atomic>
#include <stdexcept>

#ifdef _WIN32
  #ifndef _WIN32_WINNT
    #define _WIN32_WINNT 0x0601
  #endif
  #ifndef WIN32_LEAN_AND_MEAN
    #define WIN32_LEAN_AND_MEAN
  #endif
  #include <windows.h>
  #include <process.h>
#else
  #include <pthread.h>
  #include <unistd.h>
#endif

namespace flashkv {

// ==========================================
// Mutex & LockGuard (Exclusive Mutual Exclusion)
// ==========================================
class Mutex {
public:
#ifdef _WIN32
    Mutex() {
        InitializeCriticalSection(&cs_);
    }
    ~Mutex() {
        DeleteCriticalSection(&cs_);
    }
    void lock() {
        EnterCriticalSection(&cs_);
    }
    bool try_lock() {
        return TryEnterCriticalSection(&cs_) != 0;
    }
    void unlock() {
        LeaveCriticalSection(&cs_);
    }
    CRITICAL_SECTION* native_handle() {
        return &cs_;
    }
private:
    CRITICAL_SECTION cs_;
    Mutex(const Mutex&) = delete;
    Mutex& operator=(const Mutex&) = delete;
#else
    Mutex() {
        pthread_mutex_init(&mutex_, nullptr);
    }
    ~Mutex() {
        pthread_mutex_destroy(&mutex_);
    }
    void lock() {
        pthread_mutex_lock(&mutex_);
    }
    bool try_lock() {
        return pthread_mutex_trylock(&mutex_) == 0;
    }
    void unlock() {
        pthread_mutex_unlock(&mutex_);
    }
    pthread_mutex_t* native_handle() {
        return &mutex_;
    }
private:
    pthread_mutex_t mutex_;
    Mutex(const Mutex&) = delete;
    Mutex& operator=(const Mutex&) = delete;
#endif
};

class LockGuard {
public:
    explicit LockGuard(Mutex& mtx) : mtx_(mtx) {
        mtx_.lock();
    }
    ~LockGuard() {
        mtx_.unlock();
    }
private:
    Mutex& mtx_;
    LockGuard(const LockGuard&) = delete;
    LockGuard& operator=(const LockGuard&) = delete;
};

// ==========================================
// SharedMutex (Reader-Writer Lock for Shards)
// Allows concurrent readers, single writer
// ==========================================
class SharedMutex {
public:
#ifdef _WIN32
    SharedMutex() {
        InitializeSRWLock(&srw_);
    }
    ~SharedMutex() {}

    void lock() { // Exclusive lock
        AcquireSRWLockExclusive(&srw_);
    }
    void unlock() { // Exclusive unlock
        ReleaseSRWLockExclusive(&srw_);
    }
    void lock_shared() { // Shared reader lock
        AcquireSRWLockShared(&srw_);
    }
    void unlock_shared() { // Shared reader unlock
        ReleaseSRWLockShared(&srw_);
    }
    SRWLOCK* native_handle() {
        return &srw_;
    }
private:
    SRWLOCK srw_;
    SharedMutex(const SharedMutex&) = delete;
    SharedMutex& operator=(const SharedMutex&) = delete;
#else
    SharedMutex() {
        pthread_rwlock_init(&rwlock_, nullptr);
    }
    ~SharedMutex() {
        pthread_rwlock_destroy(&rwlock_);
    }
    void lock() {
        pthread_rwlock_wrlock(&rwlock_);
    }
    void unlock() {
        pthread_rwlock_unlock(&rwlock_);
    }
    void lock_shared() {
        pthread_rwlock_rdlock(&rwlock_);
    }
    void unlock_shared() {
        pthread_rwlock_unlock(&rwlock_);
    }
    pthread_rwlock_t* native_handle() {
        return &rwlock_;
    }
private:
    pthread_rwlock_t rwlock_;
    SharedMutex(const SharedMutex&) = delete;
    SharedMutex& operator=(const SharedMutex&) = delete;
#endif
};

class SharedLock {
public:
    explicit SharedLock(SharedMutex& sm) : sm_(sm) {
        sm_.lock_shared();
    }
    ~SharedLock() {
        sm_.unlock_shared();
    }
private:
    SharedMutex& sm_;
    SharedLock(const SharedLock&) = delete;
    SharedLock& operator=(const SharedLock&) = delete;
};

class UniqueLock {
public:
    explicit UniqueLock(SharedMutex& sm) : sm_(sm) {
        sm_.lock();
    }
    ~UniqueLock() {
        sm_.unlock();
    }
private:
    SharedMutex& sm_;
    UniqueLock(const UniqueLock&) = delete;
    UniqueLock& operator=(const UniqueLock&) = delete;
};

// ==========================================
// Condition Variable
// ==========================================
class ConditionVariable {
public:
#ifdef _WIN32
    ConditionVariable() {
        InitializeConditionVariable(&cv_);
    }
    ~ConditionVariable() {}

    void wait(Mutex& mtx) {
        SleepConditionVariableCS(&cv_, mtx.native_handle(), INFINITE);
    }

    bool wait_for(Mutex& mtx, uint32_t timeout_ms) {
        return SleepConditionVariableCS(&cv_, mtx.native_handle(), timeout_ms) != 0;
    }

    void notify_one() {
        WakeConditionVariable(&cv_);
    }

    void notify_all() {
        WakeAllConditionVariable(&cv_);
    }
private:
    CONDITION_VARIABLE cv_;
    ConditionVariable(const ConditionVariable&) = delete;
    ConditionVariable& operator=(const ConditionVariable&) = delete;
#else
    ConditionVariable() {
        pthread_cond_init(&cond_, nullptr);
    }
    ~ConditionVariable() {
        pthread_cond_destroy(&cond_);
    }

    void wait(Mutex& mtx) {
        pthread_cond_wait(&cond_, mtx.native_handle());
    }

    void notify_one() {
        pthread_cond_signal(&cond_);
    }

    void notify_all() {
        pthread_cond_broadcast(&cond_);
    }
private:
    pthread_cond_t cond_;
    ConditionVariable(const ConditionVariable&) = delete;
    ConditionVariable& operator=(const ConditionVariable&) = delete;
#endif
};

// ==========================================
// Thread
// ==========================================
class Thread {
public:
    Thread() : joined_(true) {
#ifdef _WIN32
        handle_ = nullptr;
#else
        thread_ = 0;
#endif
    }

    template <typename Function, typename... Args>
    explicit Thread(Function&& f, Args&&... args) : joined_(false) {
        auto task = std::bind(std::forward<Function>(f), std::forward<Args>(args)...);
        auto* task_ptr = new std::function<void()>(task);

#ifdef _WIN32
        handle_ = (HANDLE)_beginthreadex(
            nullptr,
            0,
            &Thread::win_thread_proxy,
            task_ptr,
            0,
            nullptr
        );
        if (!handle_) {
            delete task_ptr;
            throw std::runtime_error("Failed to create thread");
        }
#else
        int ret = pthread_create(&thread_, nullptr, &Thread::posix_thread_proxy, task_ptr);
        if (ret != 0) {
            delete task_ptr;
            throw std::runtime_error("Failed to create pthread");
        }
#endif
    }

    ~Thread() {
        if (joinable()) {
            join();
        }
    }

    Thread(Thread&& other) noexcept {
        joined_ = other.joined_;
#ifdef _WIN32
        handle_ = other.handle_;
        other.handle_ = nullptr;
#else
        thread_ = other.thread_;
        other.thread_ = 0;
#endif
        other.joined_ = true;
    }

    Thread& operator=(Thread&& other) noexcept {
        if (this != &other) {
            if (joinable()) {
                join();
            }
            joined_ = other.joined_;
#ifdef _WIN32
            handle_ = other.handle_;
            other.handle_ = nullptr;
#else
            thread_ = other.thread_;
            other.thread_ = 0;
#endif
            other.joined_ = true;
        }
        return *this;
    }

    bool joinable() const {
        return !joined_;
    }

    void join() {
        if (!joinable()) return;
#ifdef _WIN32
        if (handle_) {
            WaitForSingleObject(handle_, INFINITE);
            CloseHandle(handle_);
            handle_ = nullptr;
        }
#else
        if (thread_) {
            pthread_join(thread_, nullptr);
            thread_ = 0;
        }
#endif
        joined_ = true;
    }

    void detach() {
        if (!joinable()) return;
#ifdef _WIN32
        if (handle_) {
            CloseHandle(handle_);
            handle_ = nullptr;
        }
#else
        if (thread_) {
            pthread_detach(thread_);
            thread_ = 0;
        }
#endif
        joined_ = true;
    }

    static void sleep_ms(uint32_t ms) {
#ifdef _WIN32
        Sleep(ms);
#else
        usleep(ms * 1000);
#endif
    }

private:
    bool joined_;
#ifdef _WIN32
    HANDLE handle_;
    static unsigned __stdcall win_thread_proxy(void* arg) {
        auto* fn = static_cast<std::function<void()>*>(arg);
        try {
            (*fn)();
        } catch (...) {}
        delete fn;
        return 0;
    }
#else
    pthread_t thread_;
    static void* posix_thread_proxy(void* arg) {
        auto* fn = static_cast<std::function<void()>*>(arg);
        try {
            (*fn)();
        } catch (...) {}
        delete fn;
        return nullptr;
    }
#endif

    Thread(const Thread&) = delete;
    Thread& operator=(const Thread&) = delete;
};

} // namespace flashkv
