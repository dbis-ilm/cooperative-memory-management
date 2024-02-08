#include "typed_column.hpp"

#include "../core/types.hpp"

template<>
int UnencodedTypedColumn<Identifier>::cmp(const void* a, const void* b) const {
    Identifier a_val = *reinterpret_cast<const Identifier*>(a);
    Identifier b_val = *reinterpret_cast<const Identifier*>(b);
    return a_val < b_val ? -1 : static_cast<int>(a_val > b_val);
}

template<>
int UnencodedTypedColumn<Integer>::cmp(const void* a, const void* b) const {
    Integer a_val = *reinterpret_cast<const Integer*>(a);
    Integer b_val = *reinterpret_cast<const Integer*>(b);
    return a_val < b_val ? -1 : static_cast<int>(a_val > b_val);
}

#define INSTANTIATE_CHAR_CMP(n) \
template<> \
int UnencodedTypedColumn<Char<n>>::cmp(const void* a, const void* b) const { \
    return memcmp(a, b, n); \
}

// TODO: consolidate template instantiation for Char<n> types - we already have similar code  in 'unencoded_column_value_printer.cpp'
INSTANTIATE_CHAR_CMP(1)
INSTANTIATE_CHAR_CMP(2)
INSTANTIATE_CHAR_CMP(3)
INSTANTIATE_CHAR_CMP(4)
INSTANTIATE_CHAR_CMP(5)
INSTANTIATE_CHAR_CMP(6)
INSTANTIATE_CHAR_CMP(7)
INSTANTIATE_CHAR_CMP(8)
INSTANTIATE_CHAR_CMP(9)
INSTANTIATE_CHAR_CMP(10)
INSTANTIATE_CHAR_CMP(11)
INSTANTIATE_CHAR_CMP(12)
INSTANTIATE_CHAR_CMP(13)
INSTANTIATE_CHAR_CMP(14)
INSTANTIATE_CHAR_CMP(15)
INSTANTIATE_CHAR_CMP(16)
INSTANTIATE_CHAR_CMP(17)
INSTANTIATE_CHAR_CMP(18)
INSTANTIATE_CHAR_CMP(19)
INSTANTIATE_CHAR_CMP(20)
INSTANTIATE_CHAR_CMP(21)
INSTANTIATE_CHAR_CMP(22)
INSTANTIATE_CHAR_CMP(23)
INSTANTIATE_CHAR_CMP(24)
INSTANTIATE_CHAR_CMP(25)
INSTANTIATE_CHAR_CMP(50)
INSTANTIATE_CHAR_CMP(500)

#define INSTANTIATE_CMP_PLACEHOLDER(type) \
template<> \
int UnencodedTypedColumn<type>::cmp(const void*, const void*) const { \
    throw std::runtime_error("cmp not implemented for type "#type); \
}

INSTANTIATE_CMP_PLACEHOLDER(void*)
INSTANTIATE_CMP_PLACEHOLDER(Decimal<2>)
INSTANTIATE_CMP_PLACEHOLDER(Decimal<4>)
INSTANTIATE_CMP_PLACEHOLDER(Decimal<6>)
INSTANTIATE_CMP_PLACEHOLDER(Date)
INSTANTIATE_CMP_PLACEHOLDER(DateTime)