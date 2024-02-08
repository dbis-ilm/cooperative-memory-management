#pragma once

#include "../core/column_base.hpp"

template <class ValueType>
class UnencodedColumnValuePrinter : public ColumnValuePrinter {
public:
    UnencodedColumnValuePrinter(const char* value, size_t width) : value(*reinterpret_cast<const ValueType*>(value)), width(width) {}

private:
    std::ostream& print(std::ostream& os) const override;

    const ValueType& value;
    size_t width;
};