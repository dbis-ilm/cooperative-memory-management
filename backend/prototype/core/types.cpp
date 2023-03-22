#include "types.hpp"

std::ostream& operator<<(std::ostream& os, const Date& value) {
    uint32_t year = value.value >> 9;
    uint32_t month = (value.value >> 5) & 0b1111;
    uint32_t day = value.value & 0b11111;
    return os << year << "-" << month << "-" << day;
}

std::ostream& operator<<(std::ostream& os, const DateTime& value) {
    uint32_t year = value.value >> 26;
    uint32_t month = (value.value >> 22) & 0b1111;
    uint32_t day = (value.value >> 17) & 0b11111;
    uint32_t second = value.value & 0x1ffff;
    uint32_t hour = second / 3600;
    uint32_t minute = second / 60;
    second = second % 60;
    return os << year << "-" << month << "-" << day << " " << hour << ":" << minute << ":" << second;
}