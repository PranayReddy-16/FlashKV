#pragma once

#include <chrono>
#include <cstdint>
#include <string>
#include <sstream>
#include <iomanip>

namespace flashkv {

class TimeUtils {
public:
    // Current time in milliseconds since UNIX epoch
    static int64_t now_ms() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count();
    }

    // Current time in microseconds (for microbenchmarks)
    static int64_t now_us() {
        return std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now().time_since_epoch()
        ).count();
    }

    // Current time in nanoseconds
    static int64_t now_ns() {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch()
        ).count();
    }

    // ISO timestamp string
    static std::string iso_timestamp() {
        auto now = std::chrono::system_clock::now();
        auto in_time_t = std::chrono::system_clock::to_time_t(now);
        std::tm* tm_ptr = std::localtime(&in_time_t);
        char buf[64];
        if (tm_ptr && std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", tm_ptr)) {
            return std::string(buf);
        }
        return "1970-01-01 00:00:00";
    }
};

} // namespace flashkv
