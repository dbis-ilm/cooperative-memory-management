#include "unencoded_column_value_printer.hpp"

#include <iomanip>

#include "table_column.hpp"
#include "temporary_column.hpp"
#include "../core/types.hpp"


std::ostream& operator<<(std::ostream& os, const ColumnValuePrinter& printer) {
    return printer.print(os);
}

template <class ValueType>
std::ostream& UnencodedColumnValuePrinter<ValueType>::print(std::ostream& os) const {
    return os << std::setw(width) << std::setfill(' ') << value;
}

#define INSTANTIATE_PRINTER(type) \
template std::ostream& UnencodedColumnValuePrinter<type>::print(std::ostream& os) const; \
template std::unique_ptr<ColumnValuePrinter> UnencodedTypedColumn<type>::print(const char* value, size_t width) const;

INSTANTIATE_PRINTER(void*)
INSTANTIATE_PRINTER(Identifier)
INSTANTIATE_PRINTER(Integer)
INSTANTIATE_PRINTER(Decimal<2>)
INSTANTIATE_PRINTER(Decimal<4>)
INSTANTIATE_PRINTER(Decimal<6>)
INSTANTIATE_PRINTER(Char<1>)
INSTANTIATE_PRINTER(Char<2>)
INSTANTIATE_PRINTER(Char<3>)
INSTANTIATE_PRINTER(Char<4>)
INSTANTIATE_PRINTER(Char<5>)
INSTANTIATE_PRINTER(Char<6>)
INSTANTIATE_PRINTER(Char<7>)
INSTANTIATE_PRINTER(Char<8>)
INSTANTIATE_PRINTER(Char<9>)
INSTANTIATE_PRINTER(Char<10>)
INSTANTIATE_PRINTER(Char<11>)
INSTANTIATE_PRINTER(Char<12>)
INSTANTIATE_PRINTER(Char<13>)
INSTANTIATE_PRINTER(Char<14>)
INSTANTIATE_PRINTER(Char<15>)
INSTANTIATE_PRINTER(Char<16>)
INSTANTIATE_PRINTER(Char<17>)
INSTANTIATE_PRINTER(Char<18>)
INSTANTIATE_PRINTER(Char<19>)
INSTANTIATE_PRINTER(Char<20>)
INSTANTIATE_PRINTER(Char<21>)
INSTANTIATE_PRINTER(Char<22>)
INSTANTIATE_PRINTER(Char<23>)
INSTANTIATE_PRINTER(Char<24>)
INSTANTIATE_PRINTER(Char<25>)
INSTANTIATE_PRINTER(Char<50>)
INSTANTIATE_PRINTER(Char<500>)
INSTANTIATE_PRINTER(Date)
INSTANTIATE_PRINTER(DateTime)