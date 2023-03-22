#pragma once

#include <cstring>
#include <stdint.h>

inline void fast_memcpy(void* dest, const void* src, size_t count) {
    switch (count) {
        default:
            memcpy(reinterpret_cast<uint8_t*>(dest) + 8, reinterpret_cast<const uint8_t*>(src) + 8, count - 8);
            __attribute__ ((fallthrough));
        case 8:
            *(reinterpret_cast<uint32_t*>(dest) + 1) = *(reinterpret_cast<const uint32_t*>(src) + 1);
            __attribute__ ((fallthrough));
        case 7:
            *(reinterpret_cast<uint8_t*>(dest) + 6) = *(reinterpret_cast<const uint8_t*>(src) + 6);
            __attribute__ ((fallthrough));
        case 6:
            *(reinterpret_cast<uint8_t*>(dest) + 5) = *(reinterpret_cast<const uint8_t*>(src) + 5);
            __attribute__ ((fallthrough));
        case 5:
            *(reinterpret_cast<uint8_t*>(dest) + 4) = *(reinterpret_cast<const uint8_t*>(src) + 4);
            __attribute__ ((fallthrough));
        case 4:
            *reinterpret_cast<uint32_t*>(dest) = *reinterpret_cast<const uint32_t*>(src);
            break;
        case 3:
            *(reinterpret_cast<uint8_t*>(dest) + 2) = *(reinterpret_cast<const uint8_t*>(src) + 2);
            __attribute__ ((fallthrough));
        case 2:
            *reinterpret_cast<uint16_t*>(dest) = *reinterpret_cast<const uint16_t*>(src);
            break;
        case 1:
            *reinterpret_cast<uint8_t*>(dest) = *reinterpret_cast<const uint8_t*>(src);
            break;
    }
}