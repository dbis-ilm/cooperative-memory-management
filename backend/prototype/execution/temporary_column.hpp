#pragma once

#include <memory>

#include "../core/column_base.hpp"
#include "typed_column.hpp"

class TemporaryColumnBase : public virtual ColumnBase {
public:
    virtual ~TemporaryColumnBase() { }
};

template<class ValueType>
class UnencodedTemporaryColumn : public TemporaryColumnBase, public UnencodedTypedColumn<ValueType> { };