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

// common helper class used by operators to handle automatic flushing of intermediate batches to the next operator
class IntermediateHelper {
public:
    IntermediateHelper(VMCache& vmcache, size_t row_size, std::shared_ptr<OperatorBase> sink, uint32_t worker_id) : vmcache(vmcache), row_size(row_size), sink(sink), worker_id(worker_id) {
        intermediates = std::make_shared<Batch>(vmcache, row_size, worker_id);
    }

    ~IntermediateHelper() {
        flush();
    }

    inline char* addRow() {
        uint32_t row_id;
        char* loc = reinterpret_cast<char*>(intermediates->addRowIfPossible(row_id));
        if (loc == nullptr) {
            sink->push(intermediates, worker_id);
            if (intermediates.use_count() > 1)
                intermediates = std::make_shared<Batch>(vmcache, row_size, worker_id);
            else
                intermediates->clear();
            loc = reinterpret_cast<char*>(intermediates->addRowIfPossible(row_id));
        }
        assert(loc);
        return loc;
    }

    inline void flush() {
        if (intermediates->getCurrentSize() > 0)
            sink->push(intermediates, worker_id);
    }

private:
    VMCache& vmcache;
    const size_t row_size;
    const std::shared_ptr<OperatorBase> sink;
    const uint32_t worker_id;
    std::shared_ptr<Batch> intermediates;
};