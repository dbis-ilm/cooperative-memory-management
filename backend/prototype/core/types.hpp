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

    CompositeKey() {
        for (size_t i = 0; i < n; i++)
            keys[i] = 0;
    }

    CompositeKey(std::initializer_list<Identifier> l) {
        if (l.size() != n) throw 0;
        auto it = l.begin();
        for (size_t i = 0; i < n; i++)
            keys[i] = *it++;
    }

    CompositeKey(Identifier value) {
        for (size_t i = 0; i < n; i++)
            keys[i] = value;
    }

    bool operator!=(const CompositeKey& other) const {
        return memcmp(keys, other.keys, n * sizeof(Identifier)) != 0;
    }

    bool operator==(const CompositeKey& other) const {
        return memcmp(keys, other.keys, n * sizeof(Identifier)) == 0;
    }

    bool operator<(const CompositeKey& other) const {
        for (size_t i = 0; i < n; i++) {
            if (keys[i] < other.keys[i])
                return true;
            if (keys[i] > other.keys[i])
                return false;
        }
        return false;
    }

    bool operator>(const CompositeKey& other) const {
        for (size_t i = 0; i < n; i++) {
            if (keys[i] > other.keys[i])
                return true;
            if (keys[i] < other.keys[i])
                return false;
        }
        return false;
    }

    bool operator>=(const CompositeKey& other) const {
        for (size_t i = 0; i < n; i++) {
            if (keys[i] < other.keys[i])
                return false;
        }
        return true;
    }

    CompositeKey operator+(Identifier op) const {
        CompositeKey result = *this;
        for (long i = n - 1; i >= 0; i--) {
            Identifier prev = result.keys[i];
            result.keys[i] += op;
            if (prev > result.keys[i]) // overflow, carry over to i - 1
                op = 1;
            else
                break;
        }
        return result;
    }

    CompositeKey operator-(Identifier op) const {
        CompositeKey result = *this;
        for (long i = n - 1; i >= 0; i--) {
            Identifier prev = result.keys[i];
            result.keys[i] -= op;
            if (prev < result.keys[i]) // overflow, carry over to i - 1
                op = 1;
            else
                break;
        }
        return result;
    }
};

namespace std {
    template<> class numeric_limits<CompositeKey<2>> {
    public:
       static CompositeKey<2> min() { return CompositeKey<2>(std::numeric_limits<Identifier>::min()); }
       static CompositeKey<2> max() { return CompositeKey<2>(std::numeric_limits<Identifier>::max()); }
    };
}

template <size_t decimals>
class Decimal {
public:
    Decimal(int64_t value) : value(value) { }

    template <size_t dec>
    friend std::ostream& operator<<(std::ostream& os, const Decimal<dec>& value);

    Decimal<decimals> operator+=(const Decimal<decimals>& rhs) {
        value += rhs.value;
        return *this;
    }

    Decimal<decimals> operator-=(const Decimal<decimals>& rhs) {
        value -= rhs.value;
        return *this;
    }

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