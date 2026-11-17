#pragma once

#include <string>
#include <vector>
#include <algorithm>
#include <cctype>
#include <cstdlib>
#include "protocol/resp.hpp"

namespace flashkv {

class CommandParser {
public:
    enum class ParseStatus {
        SUCCESS,
        INCOMPLETE,
        ERROR
    };

    // Attempts to parse one full command from the input buffer.
    // On SUCCESS: consumed bytes are removed from in_buffer, args filled with command + arguments.
    // On INCOMPLETE: in_buffer is untouched, wait for more data from socket.
    // On ERROR: malformed data.
    static ParseStatus parse(std::string& in_buffer, std::vector<std::string>& out_args, std::string& err_msg) {
        if (in_buffer.empty()) {
            return ParseStatus::INCOMPLETE;
        }

        // Check if RESP Array format (*<count>\r\n...)
        if (in_buffer[0] == '*') {
            return parse_resp_array(in_buffer, out_args, err_msg);
        }

        // Otherwise parse as Inline Command (e.g. "SET foo bar\r\n")
        return parse_inline_command(in_buffer, out_args, err_msg);
    }

    // Helper: uppercase string
    static std::string to_upper(std::string s) {
        for (char& c : s) {
            c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
        }
        return s;
    }

private:
    static ParseStatus parse_resp_array(std::string& in_buffer, std::vector<std::string>& out_args, std::string& err_msg) {
        size_t line_end = in_buffer.find("\r\n");
        if (line_end == std::string::npos) {
            return ParseStatus::INCOMPLETE;
        }

        // Parse array count
        std::string count_str = in_buffer.substr(1, line_end - 1);
        char* endptr = nullptr;
        long num_elements = std::strtol(count_str.c_str(), &endptr, 10);
        if (*endptr != '\0' || num_elements < 0 || num_elements > 1024 * 1024) {
            err_msg = "Protocol error: invalid multibulk length";
            return ParseStatus::ERROR;
        }

        if (num_elements == 0) {
            in_buffer.erase(0, line_end + 2);
            return ParseStatus::SUCCESS;
        }

        size_t cursor = line_end + 2;
        std::vector<std::string> parsed;
        parsed.reserve(static_cast<size_t>(num_elements));

        for (long i = 0; i < num_elements; ++i) {
            if (cursor >= in_buffer.size()) {
                return ParseStatus::INCOMPLETE;
            }

            if (in_buffer[cursor] != '$') {
                err_msg = "Protocol error: expected '$' for bulk string";
                return ParseStatus::ERROR;
            }

            size_t bulk_line_end = in_buffer.find("\r\n", cursor);
            if (bulk_line_end == std::string::npos) {
                return ParseStatus::INCOMPLETE;
            }

            std::string len_str = in_buffer.substr(cursor + 1, bulk_line_end - (cursor + 1));
            long bulk_len = std::strtol(len_str.c_str(), &endptr, 10);
            if (*endptr != '\0') {
                err_msg = "Protocol error: invalid bulk length";
                return ParseStatus::ERROR;
            }

            if (bulk_len < 0) {
                // Null bulk string
                parsed.emplace_back("");
                cursor = bulk_line_end + 2;
                continue;
            }

            size_t str_start = bulk_line_end + 2;
            size_t total_needed = str_start + static_cast<size_t>(bulk_len) + 2; // + \r\n
            if (in_buffer.size() < total_needed) {
                return ParseStatus::INCOMPLETE;
            }

            if (in_buffer[str_start + bulk_len] != '\r' || in_buffer[str_start + bulk_len + 1] != '\n') {
                err_msg = "Protocol error: missing CRLF after bulk string";
                return ParseStatus::ERROR;
            }

            parsed.emplace_back(in_buffer.substr(str_start, static_cast<size_t>(bulk_len)));
            cursor = total_needed;
        }

        // Successfully parsed full RESP array!
        in_buffer.erase(0, cursor);
        out_args = std::move(parsed);
        if (!out_args.empty()) {
            out_args[0] = to_upper(out_args[0]);
        }
        return ParseStatus::SUCCESS;
    }

    static ParseStatus parse_inline_command(std::string& in_buffer, std::vector<std::string>& out_args, std::string& /*err_msg*/) {
        size_t newline_pos = in_buffer.find('\n');
        if (newline_pos == std::string::npos) {
            return ParseStatus::INCOMPLETE;
        }

        size_t line_length = newline_pos;
        if (line_length > 0 && in_buffer[line_length - 1] == '\r') {
            line_length--;
        }

        std::string line = in_buffer.substr(0, line_length);
        in_buffer.erase(0, newline_pos + 1);

        // Tokenize space-separated arguments (supporting quotes)
        std::vector<std::string> tokens;
        size_t i = 0;
        while (i < line.size()) {
            while (i < line.size() && (line[i] == ' ' || line[i] == '\t')) {
                i++;
            }
            if (i >= line.size()) break;

            if (line[i] == '"' || line[i] == '\'') {
                char quote = line[i++];
                std::string token;
                while (i < line.size() && line[i] != quote) {
                    if (line[i] == '\\' && i + 1 < line.size()) {
                        i++;
                        token += line[i++];
                    } else {
                        token += line[i++];
                    }
                }
                if (i < line.size() && line[i] == quote) {
                    i++;
                }
                tokens.push_back(token);
            } else {
                size_t start = i;
                while (i < line.size() && line[i] != ' ' && line[i] != '\t') {
                    i++;
                }
                tokens.push_back(line.substr(start, i - start));
            }
        }

        if (tokens.empty()) {
            return ParseStatus::INCOMPLETE; // Empty line, ignore
        }

        tokens[0] = to_upper(tokens[0]);
        out_args = std::move(tokens);
        return ParseStatus::SUCCESS;
    }
};

} // namespace flashkv
