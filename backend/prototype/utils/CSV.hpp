#pragma once
#include <iostream>
#include <vector>

inline uint64_t parse_int(const char* str, size_t len) {
    uint64_t result = 0;
    for (size_t i = 0; i < len; i++) {
        const char c = str[i];
        if (c < '0' || c > '9')
            throw std::runtime_error("Invalid character encountered while parsing int!");
        result *= 10;
        result += static_cast<uint64_t>(c - '0');
    }
    return result;
}

inline int64_t parse_decimal(const char* str, size_t len, size_t decimals) {
    int64_t result = 0;
    bool decimal_point = false;
    bool negative = false;
    size_t read_decimals = 0;
    for (size_t i = 0; i < len; i++) {
        const char c = str[i];
        if (c == '.') {
            if (!decimal_point) {
                decimal_point = true;
            } else {
                throw std::runtime_error("Decimal contained multiple points!");
            }
        } else {
            if (i == 0 && c == '-') {
                negative = true;
                continue;
            } else if (c < '0' || c > '9')
                throw std::runtime_error("Invalid character encountered while parsing decimal!");
            result *= 10;
            result += static_cast<int64_t>(c - '0');
            if (decimal_point)
                read_decimals++;
        }
    }
    if (read_decimals > decimals) {
        throw std::runtime_error("Read more decimal digits than expected!");
    }
    while (read_decimals != decimals) {
        result *= 10;
        read_decimals++;
    }
    return negative ? -result : result;
}

// uint32_t date encoding (starting from LSB):
// bit 0-4: day
// bit 5-8: month
// remaining bits: year

/// decode as follows:
//uint32_t year = date >> 9;
//uint32_t month = (date >> 5) & 0b1111;
//uint32_t day = date & 0b11111;
//std::cout << year << std::endl;
//std::cout << month << std::endl;
//std::cout << day << std::endl;

inline uint32_t encode_date(uint32_t year, uint32_t month, uint32_t day) {
    return day | (month << 5) | (year << 9);
}

// parses a date in ISO 8601 format (YYYY-MM-DD) from string and returns it encoded according to the encoding defined above
inline uint32_t parse_date(const char* str, size_t len) {
    if (len != 10 || str[4] != '-' || str[7] != '-')
        throw std::runtime_error("Malformed date string!");

    uint32_t year = parse_decimal(str, 4, 0);
    uint32_t month = parse_decimal(str + 5, 2, 0);
    uint32_t day = parse_decimal(str + 8, 2, 0);

    return encode_date(year, month, day);
}

// uint64_t datetime encoding (starting from LSB):
// bit 0-16: second of day
// bit 17-21: day
// bit 22-25: month
// remaining bits: year

/// decode as follows:
//uint32_t year = date >> 26;
//uint32_t month = (date >> 22) & 0b1111;
//uint32_t day = (date >> 17) & 0b11111;
//uint32_t second = date & 0x1ffff;

inline uint64_t encode_date_time(uint32_t year, uint32_t month, uint32_t day, uint32_t hour, uint32_t minute, uint32_t second) {
    return static_cast<uint64_t>((second + minute * 60 + hour * 3600) | (day << 17) | (month << 22)) | (static_cast<uint64_t>(year) << 26);
}

// parses a date & time in ISO 8601 format (YYYY-MM-DD hh:mm:ss) from string and returns it encoded according to the encoding defined above
inline uint64_t parse_date_time(const char* str, size_t len) {
    // TODO: add proper NULL handling
    if (len == 0)
        return 0;
    if (len != 19 || str[4] != '-' || str[7] != '-' || str[10] != ' ' || str[13] != ':' || str[16] != ':')
        throw std::runtime_error("Malformed datetime string!");

    uint32_t year = parse_decimal(str, 4, 0);
    uint32_t month = parse_decimal(str + 5, 2, 0);
    uint32_t day = parse_decimal(str + 8, 2, 0);
    uint32_t hour = parse_decimal(str + 11, 2, 0);
    uint32_t minute = parse_decimal(str + 14, 2, 0);
    uint32_t second = parse_decimal(str + 17, 2, 0);

    return encode_date_time(year, month, day, hour, minute, second);
}

enum class ParseType {
    Skip,
    Int32,
    Date,
    DateTime,
    Decimal,
    Char
};

struct ParseTypeDescription {
    ParseType type;
    union params {
        size_t decimals;
        size_t len;
    } params;

    static ParseTypeDescription Skip() { return { ParseType::Skip, .params = {} }; }
    static ParseTypeDescription Int32() { return { ParseType::Int32, .params = {} }; }
    static ParseTypeDescription Date() { return { ParseType::Date, .params = {} }; }
    static ParseTypeDescription DateTime() { return { ParseType::DateTime, .params = {} }; }
    static ParseTypeDescription Decimal(size_t decimals) { return { ParseType::Decimal, .params = { decimals } }; }
    static ParseTypeDescription Char(size_t len) { return { ParseType::Char, .params = { len } }; }
};

size_t parse_csv_chunk(std::istream& csv, size_t offset, std::streamoff length, const char sep, std::vector<ParseTypeDescription> types, std::vector<void*> destinations) {
    if (offset != 0) {
        csv.seekg(offset - 1, std::ios::beg);
        length++;
    } else {
        csv.seekg(0, std::ios::beg);
    }
    auto fbeg = csv.tellg();

    std::string line;
    if (offset != 0) {
        getline(csv, line); // skip first (incomplete) line
    }
    size_t parsed_rows = 0;
    while (csv.tellg() < fbeg + length && !getline(csv, line).eof()) {
        size_t word_i = 0;
        size_t start = 0;
        for (size_t i = 0; i < line.length(); i++) {
            if (line[i] == sep || i == line.length() - 1) {
                if (i == line.length() - 1 && line[i] != sep)
                    i++;
                if (word_i >= types.size())
                    throw std::runtime_error("CSV contains more columns than specified using 'types'");
                const char* word = line.c_str() + start;
                const size_t word_len = i - start;
                const auto& parse_type = types[word_i];
                switch (parse_type.type) {
                    case ParseType::Skip:
                        break;
                    case ParseType::Int32:
                        reinterpret_cast<std::vector<uint32_t>*>(destinations[word_i])->push_back(parse_int(word, word_len));
                        break;
                    case ParseType::Date:
                        reinterpret_cast<std::vector<uint32_t>*>(destinations[word_i])->push_back(parse_date(word, word_len));
                        break;
                    case ParseType::DateTime:
                        reinterpret_cast<std::vector<uint64_t>*>(destinations[word_i])->push_back(parse_date_time(word, word_len));
                        break;
                    case ParseType::Decimal:
                        reinterpret_cast<std::vector<int64_t>*>(destinations[word_i])->push_back(parse_decimal(word, word_len, parse_type.params.decimals));
                        break;
                    case ParseType::Char:
                        if (word_len > parse_type.params.len)
                            throw std::runtime_error("Parsed string is longer than specified");
                        for (size_t j = 0; j < word_len; j++)
                            reinterpret_cast<std::vector<char>*>(destinations[word_i])->push_back(word[j]);
                        for (size_t j = word_len; j < parse_type.params.len; j++)
                            reinterpret_cast<std::vector<char>*>(destinations[word_i])->push_back(0);
                        break;
                }
                word_i++;
                start = i + 1;
            }
        }
        parsed_rows++;
    }
    return parsed_rows;
}