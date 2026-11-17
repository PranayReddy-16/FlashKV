#pragma once

#include <string>
#include <vector>
#include <sstream>
#include <cstdint>

#ifdef ERROR
  #undef ERROR
#endif

namespace flashkv {

enum class RespType {
    SIMPLE_STRING,
    ERR,
    INTEGER,
    BULK_STRING,
    ARRAY,
    NULL_BULK_STRING,
    NULL_ARRAY
};

struct RespValue {
    RespType type;
    std::string str_val;
    int64_t int_val;
    std::vector<RespValue> elements;

    RespValue() : type(RespType::SIMPLE_STRING), int_val(0) {}

    static RespValue make_simple_string(const std::string& str) {
        RespValue v;
        v.type = RespType::SIMPLE_STRING;
        v.str_val = str;
        return v;
    }

    static RespValue make_ok() {
        return make_simple_string("OK");
    }

    static RespValue make_pong() {
        return make_simple_string("PONG");
    }

    static RespValue make_error(const std::string& err_msg) {
        RespValue v;
        v.type = RespType::ERR;
        v.str_val = err_msg;
        return v;
    }

    static RespValue make_integer(int64_t num) {
        RespValue v;
        v.type = RespType::INTEGER;
        v.int_val = num;
        return v;
    }

    static RespValue make_bulk_string(const std::string& str) {
        RespValue v;
        v.type = RespType::BULK_STRING;
        v.str_val = str;
        return v;
    }

    static RespValue make_null_bulk_string() {
        RespValue v;
        v.type = RespType::NULL_BULK_STRING;
        return v;
    }

    static RespValue make_array(const std::vector<RespValue>& elems = {}) {
        RespValue v;
        v.type = RespType::ARRAY;
        v.elements = elems;
        return v;
    }

    static RespValue make_null_array() {
        RespValue v;
        v.type = RespType::NULL_ARRAY;
        return v;
    }

    // Serialize this RespValue into standard RESP wire format
    std::string serialize() const {
        std::string out;
        serialize_to(out);
        return out;
    }

    void serialize_to(std::string& out) const {
        switch (type) {
            case RespType::SIMPLE_STRING:
                out.reserve(out.size() + 3 + str_val.size());
                out += '+';
                out += str_val;
                out += "\r\n";
                break;
            case RespType::ERR:
                out.reserve(out.size() + 3 + str_val.size());
                out += '-';
                out += str_val;
                out += "\r\n";
                break;
            case RespType::INTEGER: {
                std::string s = std::to_string(int_val);
                out.reserve(out.size() + 3 + s.size());
                out += ':';
                out += s;
                out += "\r\n";
                break;
            }
            case RespType::BULK_STRING: {
                std::string len = std::to_string(str_val.size());
                out.reserve(out.size() + 3 + len.size() + 2 + str_val.size() + 2);
                out += '$';
                out += len;
                out += "\r\n";
                out += str_val;
                out += "\r\n";
                break;
            }
            case RespType::NULL_BULK_STRING:
                out += "$-1\r\n";
                break;
            case RespType::ARRAY: {
                std::string len = std::to_string(elements.size());
                out += '*';
                out += len;
                out += "\r\n";
                for (const auto& elem : elements) {
                    elem.serialize_to(out);
                }
                break;
            }
            case RespType::NULL_ARRAY:
                out += "*-1\r\n";
                break;
        }
    }
};

} // namespace flashkv
