#pragma once

#include <cstddef>
#include <iostream>
#include <memory>

class ColumnValuePrinter {
public:
    virtual ~ColumnValuePrinter() { }
    friend std::ostream& operator<<(std::ostream& os, const ColumnValuePrinter& printer);

private:
    virtual std::ostream& print(std::ostream& os) const = 0;
};

std::ostream& operator<<(std::ostream& os, const ColumnValuePrinter& printer);

class ColumnBase {
public:
    virtual ~ColumnBase() { }
    virtual size_t getValueTypeSize() const = 0;
    virtual int cmp(const void* a, const void* b) const = 0;
    virtual std::unique_ptr<ColumnValuePrinter> print(const char* value, size_t width) const = 0;
};