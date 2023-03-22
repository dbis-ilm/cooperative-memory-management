#pragma once

#include <memory>

#include "batch.hpp"

class OperatorBase {
protected:
    std::shared_ptr<OperatorBase> next_operator = nullptr;

public:
    OperatorBase();
    virtual ~OperatorBase();

    virtual void push(std::shared_ptr<Batch> batch, uint32_t worker_id) = 0;

    void setNextOperator(std::shared_ptr<OperatorBase> next_operator);

    std::shared_ptr<OperatorBase> getNextOperator() const;
};