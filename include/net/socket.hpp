#pragma once

#include <string>
#include <vector>
#include <cstdint>
#include <iostream>
#include <cstring>

#ifdef _WIN32
  #ifndef _WIN32_WINNT
    #define _WIN32_WINNT 0x0601
  #endif
  #ifndef WIN32_LEAN_AND_MEAN
    #define WIN32_LEAN_AND_MEAN
  #endif
  #include <winsock2.h>
  #include <ws2tcpip.h>
  typedef SOCKET socket_t;
  #define IS_INVALID_SOCKET(s) ((s) == INVALID_SOCKET)
  #define CLOSE_SOCKET(s) closesocket(s)
  #define SOCKET_ERRNO WSAGetLastError()
#else
  #include <sys/types.h>
  #include <sys/socket.h>
  #include <netinet/in.h>
  #include <netinet/tcp.h>
  #include <arpa/inet.h>
  #include <netdb.h>
  #include <unistd.h>
  #include <fcntl.h>
  #include <errno.h>
  typedef int socket_t;
  #define INVALID_SOCKET (-1)
  #define SOCKET_ERROR (-1)
  #define IS_INVALID_SOCKET(s) ((s) < 0)
  #define CLOSE_SOCKET(s) ::close(s)
  #define SOCKET_ERRNO errno
#endif

namespace flashkv {

class SocketPlatform {
public:
    static void init() {
#ifdef _WIN32
        WSADATA wsa_data;
        int res = WSAStartup(MAKEWORD(2, 2), &wsa_data);
        if (res != 0) {
            std::cerr << "WSAStartup failed with error: " << res << std::endl;
        }
#endif
    }

    static void cleanup() {
#ifdef _WIN32
        WSACleanup();
#endif
    }
};

class Socket {
public:
    Socket() : fd_(INVALID_SOCKET) {}
    explicit Socket(socket_t fd) : fd_(fd) {}

    ~Socket() {
        close();
    }

    Socket(Socket&& other) noexcept : fd_(other.fd_) {
        other.fd_ = INVALID_SOCKET;
    }

    Socket& operator=(Socket&& other) noexcept {
        if (this != &other) {
            close();
            fd_ = other.fd_;
            other.fd_ = INVALID_SOCKET;
        }
        return *this;
    }

    bool is_valid() const {
        return !IS_INVALID_SOCKET(fd_);
    }

    socket_t release() {
        socket_t ret = fd_;
        fd_ = INVALID_SOCKET;
        return ret;
    }

    socket_t native_handle() const {
        return fd_;
    }

    bool open_tcp() {
        close();
        fd_ = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        return is_valid();
    }

    void close() {
        if (is_valid()) {
            CLOSE_SOCKET(fd_);
            fd_ = INVALID_SOCKET;
        }
    }

    bool set_reuse_addr(bool enable = true) {
        if (!is_valid()) return false;
        int opt = enable ? 1 : 0;
        return setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&opt), sizeof(opt)) == 0;
    }

    // Disable Nagle's algorithm for microsecond request/response latency
    bool set_tcp_nodelay(bool enable = true) {
        if (!is_valid()) return false;
        int opt = enable ? 1 : 0;
        return setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<const char*>(&opt), sizeof(opt)) == 0;
    }

    bool set_timeout(int timeout_ms) {
        if (!is_valid()) return false;
#ifdef _WIN32
        DWORD tv = static_cast<DWORD>(timeout_ms);
        setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof(tv));
        setsockopt(fd_, SOL_SOCKET, SO_SNDTIMEO, (const char*)&tv, sizeof(tv));
        return true;
#else
        struct timeval tv;
        tv.tv_sec = timeout_ms / 1000;
        tv.tv_usec = (timeout_ms % 1000) * 1000;
        setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        setsockopt(fd_, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
        return true;
#endif
    }

    bool bind_and_listen(const std::string& host, int port, int backlog = 1024) {
        if (!open_tcp()) return false;
        set_reuse_addr(true);
        set_tcp_nodelay(true);

        sockaddr_in addr;
        std::memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_port = htons(static_cast<uint16_t>(port));

        if (host.empty() || host == "0.0.0.0") {
            addr.sin_addr.s_addr = htonl(INADDR_ANY);
        } else {
            addr.sin_addr.s_addr = inet_addr(host.c_str());
        }

        if (::bind(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == SOCKET_ERROR) {
            close();
            return false;
        }

        if (::listen(fd_, backlog) == SOCKET_ERROR) {
            close();
            return false;
        }

        return true;
    }

    Socket accept(std::string* client_ip = nullptr, int* client_port = nullptr) {
        sockaddr_in client_addr;
        #ifdef _WIN32
        int addr_len = sizeof(client_addr);
        #else
        socklen_t addr_len = sizeof(client_addr);
        #endif

        socket_t client_fd = ::accept(fd_, reinterpret_cast<sockaddr*>(&client_addr), &addr_len);
        if (IS_INVALID_SOCKET(client_fd)) {
            return Socket();
        }

        if (client_ip) {
            char* ip = inet_ntoa(client_addr.sin_addr);
            *client_ip = ip ? ip : "127.0.0.1";
        }
        if (client_port) {
            *client_port = ntohs(client_addr.sin_port);
        }

        Socket s(client_fd);
        s.set_tcp_nodelay(true);
        return s;
    }

    bool connect(const std::string& host, int port) {
        if (!open_tcp()) return false;
        set_tcp_nodelay(true);

        sockaddr_in addr;
        std::memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_port = htons(static_cast<uint16_t>(port));
        addr.sin_addr.s_addr = inet_addr(host.c_str());

        if (addr.sin_addr.s_addr == INADDR_NONE) {
            // Resolve hostname
            struct hostent* he = gethostbyname(host.c_str());
            if (!he) {
                close();
                return false;
            }
            std::memcpy(&addr.sin_addr, he->h_addr_list[0], he->h_length);
        }

        if (::connect(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == SOCKET_ERROR) {
            close();
            return false;
        }
        return true;
    }

    int send_all(const char* data, size_t length) {
        size_t total_sent = 0;
        while (total_sent < length) {
            int sent = ::send(fd_, data + total_sent, static_cast<int>(length - total_sent), 0);
            if (sent <= 0) {
                return -1;
            }
            total_sent += static_cast<size_t>(sent);
        }
        return static_cast<int>(total_sent);
    }

    int send_all(const std::string& str) {
        return send_all(str.data(), str.size());
    }

    int recv_some(char* buffer, size_t max_length) {
        return ::recv(fd_, buffer, static_cast<int>(max_length), 0);
    }

private:
    socket_t fd_;

    Socket(const Socket&) = delete;
    Socket& operator=(const Socket&) = delete;
};

} // namespace flashkv
