#pragma once

#include <cstddef>
#include <iostream>
#include <memory>

#include "../core/column_base.hpp"

class ColumnValuePrinter {
public:
    virtual ~ColumnValuePrinter() { }
    friend std::ostream& operator<<(std::ostream& os, const ColumnValuePrinter& printer);

private:
    virtual std::ostream& print(std::ostream& os) const = 0;
};

std::ostream& operator<<(std::ostream& os, const ColumnValuePrinter& printer);

class TemporaryColumnBase : public virtual ColumnBase {
public:
    virtual ~TemporaryColumnBase() { }
    virtual size_t getValueTypeSize() const = 0;
    virtual std::unique_ptr<ColumnValuePrinter> print(const char* value, size_t width) const = 0;
};

template<class ValueType>
class UnencodedTemporaryColumn : public TemporaryColumnBase {
public:
    size_t getValueTypeSize() const override {
        return sizeof(ValueType);
    }

    std::unique_ptr<ColumnValuePrinter> print(const char* value, size_t width) const override;
};