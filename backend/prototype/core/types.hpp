#pragma once

#include <cstddef>
#include <cstring>
#include <iomanip>
#include <iostream>

typedef int32_t Integer;
typedef uint32_t Identifier;

template <size_t n>
struct CompositeKey {
    Identifier keys[n];

    bool operator!=(const CompositeKey& other) const {
        return memcmp(keys, other.keys, n * sizeof(Identifier)) != 0;
    }

    bool operator==(const CompositeKey& other) const {
        return memcmp(keys, other.keys, n * sizeof(Identifier)) == 0;
    }

    bool operator<(const CompositeKey& other) const {
        return memcmp(keys, other.keys, n * sizeof(Identifier)) < 0;
    }

    bool operator>(const CompositeKey& other) const {
        return memcmp(keys, other.keys, n * sizeof(Identifier)) > 0;
    }
};

template <size_t decimals>
class Decimal {
public:
    Decimal(int64_t value) : value(value) { }

    template <size_t dec>
    friend std::ostream& operator<<(std::ostream& os, const Decimal<dec>& value);

private:
    int64_t value;
};

template <size_t decimals>
std::ostream& operator<<(std::ostream& os, const Decimal<decimals>& value) {
    int64_t fac = 1;
    for (size_t i = 0; i < decimals; i++) {
        fac *= 10;
    }
    return os << std::setw(os.width() - decimals - 1) << std::setfill(' ') << value.value / fac << "." << std::setw(decimals) << std::setfill('0') << value.value % fac;
}

template <size_t length>
class Char {
    template <size_t len>
    friend std::ostream& operator<<(std::ostream& os, const Char<len>& value);

private:
    char value[length];
};

template <size_t length>
std::ostream& operator<<(std::ostream& os, const Char<length>& value) {
    char print_value[length + 1];
    print_value[length] = 0;
    memcpy(print_value, value.value, length);
    return os << print_value;
}

class Date {
public:
    Date(uint32_t value) : value(value) { }

    friend std::ostream& operator<<(std::ostream& os, const Date& value);

private:
    uint32_t value;
};

class DateTime {
public:
    DateTime(uint64_t value) : value(value) { }

    uint32_t getYear() const { return value >> 26; }
    uint32_t getMonth() const { return (value >> 22) & 0b1111; }
    uint32_t getDay() const { return (value >> 17) & 0b11111; }
    uint32_t getSecond() const { return value & 0x1ffff; }

    friend std::ostream& operator<<(std::ostream& os, const DateTime& value);

private:
    uint64_t value;
};