#pragma once

#include <string>
#include <vector>
#include <chrono>
#include <iostream>
#include "net/socket.hpp"
#include "protocol/resp.hpp"
#include "protocol/command_parser.hpp"

namespace flashkv {

class TcpClient {
public:
    TcpClient() : connected_(false) {}

    ~TcpClient() {
        disconnect();
    }

    bool connect(const std::string& host, int port) {
        disconnect();
        if (!socket_.connect(host, port)) {
            return false;
        }
        connected_ = true;
        in_buffer_.clear();
        return true;
    }

    void disconnect() {
        if (connected_) {
            socket_.close();
            connected_ = false;
        }
    }

    bool is_connected() const {
        return connected_;
    }

    // Send command as RESP array and receive parsed RESP response
    bool send_command(const std::vector<std::string>& args, RespValue& out_resp, std::string& err_msg) {
        if (!connected_) {
            err_msg = "Client not connected";
            return false;
        }

        // Format as RESP array
        std::vector<RespValue> elements;
        elements.reserve(args.size());
        for (const auto& a : args) {
            elements.push_back(RespValue::make_bulk_string(a));
        }
        std::string request = RespValue::make_array(elements).serialize();

        if (socket_.send_all(request) < 0) {
            err_msg = "Failed to send request";
            connected_ = false;
            return false;
        }

        return read_response(out_resp, err_msg);
    }

    // Send raw inline command string (e.g. "SET foo bar\r\n")
    bool send_raw(const std::string& raw_cmd, RespValue& out_resp, std::string& err_msg) {
        if (!connected_) {
            err_msg = "Client not connected";
            return false;
        }

        std::string payload = raw_cmd;
        if (payload.find('\n') == std::string::npos) {
            payload += "\r\n";
        }

        if (socket_.send_all(payload) < 0) {
            err_msg = "Failed to send raw request";
            connected_ = false;
            return false;
        }

        return read_response(out_resp, err_msg);
    }

    // Read full RESP response from socket
    bool read_response(RespValue& out_resp, std::string& err_msg) {
        char chunk[4096];
        while (true) {
            // Try to parse from in_buffer_
            if (!in_buffer_.empty()) {
                size_t consumed = 0;
                if (parse_single_resp(in_buffer_, out_resp, consumed, err_msg)) {
                    in_buffer_.erase(0, consumed);
                    return true;
                }
                if (!err_msg.empty()) {
                    return false; // Real parse error
                }
            }

            int bytes = socket_.recv_some(chunk, sizeof(chunk));
            if (bytes <= 0) {
                err_msg = "Connection closed by peer";
                connected_ = false;
                return false;
            }
            in_buffer_.append(chunk, static_cast<size_t>(bytes));
        }
    }

private:
    bool parse_single_resp(const std::string& buf, RespValue& out_resp, size_t& consumed, std::string& err_msg) {
        if (buf.empty()) return false;
        char type = buf[0];

        if (type == '+' || type == '-' || type == ':') {
            size_t crlf = buf.find("\r\n");
            if (crlf == std::string::npos) return false;
            std::string line = buf.substr(1, crlf - 1);
            consumed = crlf + 2;

            if (type == '+') {
                out_resp = RespValue::make_simple_string(line);
            } else if (type == '-') {
                out_resp = RespValue::make_error(line);
            } else {
                out_resp = RespValue::make_integer(std::strtoll(line.c_str(), nullptr, 10));
            }
            return true;
        }

        if (type == '$') {
            size_t crlf = buf.find("\r\n");
            if (crlf == std::string::npos) return false;
            std::string len_str = buf.substr(1, crlf - 1);
            long len = std::strtol(len_str.c_str(), nullptr, 10);
            if (len == -1) {
                out_resp = RespValue::make_null_bulk_string();
                consumed = crlf + 2;
                return true;
            }
            size_t data_start = crlf + 2;
            size_t total = data_start + static_cast<size_t>(len) + 2;
            if (buf.size() < total) return false;

            out_resp = RespValue::make_bulk_string(buf.substr(data_start, static_cast<size_t>(len)));
            consumed = total;
            return true;
        }

        if (type == '*') {
            size_t crlf = buf.find("\r\n");
            if (crlf == std::string::npos) return false;
            std::string count_str = buf.substr(1, crlf - 1);
            long count = std::strtol(count_str.c_str(), nullptr, 10);
            if (count == -1) {
                out_resp = RespValue::make_null_array();
                consumed = crlf + 2;
                return true;
            }

            size_t cur = crlf + 2;
            std::vector<RespValue> elements;
            elements.reserve(static_cast<size_t>(count));

            for (long i = 0; i < count; ++i) {
                std::string sub = buf.substr(cur);
                RespValue sub_resp;
                size_t sub_consumed = 0;
                if (!parse_single_resp(sub, sub_resp, sub_consumed, err_msg)) {
                    return false;
                }
                elements.push_back(std::move(sub_resp));
                cur += sub_consumed;
            }

            out_resp = RespValue::make_array(elements);
            consumed = cur;
            return true;
        }

        err_msg = "Unknown RESP prefix byte: " + std::string(1, type);
        return false;
    }

    Socket socket_;
    bool connected_;
    std::string in_buffer_;
};

} // namespace flashkv
