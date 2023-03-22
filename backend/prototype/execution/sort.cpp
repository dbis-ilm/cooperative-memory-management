#include "sort.hpp"

#include <cstdlib>

SortBreaker::SortBreaker(BatchDescription& batch_description, const std::vector<NamedColumn>& sort_keys, const std::vector<Order>& sort_orders, size_t num_workers)
: DefaultBreaker(batch_description, num_workers)
, sort_keys(sort_keys)
, sort_key_infos(sort_keys.size())
, sort_orders(sort_orders) {
    if (sort_keys.size() != sort_orders.size())
        throw std::runtime_error("Invalid sort specification, sort_keys.size() must equal sort_orders.size()!");
    // ensure that the sort keys are present in the input columns
    size_t i = 0;
    for (const auto& key : sort_keys) {
        if (!this->batch_description.tryFind(key.name, sort_key_infos[i++]))
            throw std::runtime_error("Sort key is missing from input columns!");
    }
    assert(sort_key_infos.size() == sort_keys.size());

    comp = [=](const Row& a, const Row& b) -> int {
        int cmp = 0;
        size_t i = 0;
        while (cmp == 0 && i < sort_key_infos.size()) {
            const auto& order = sort_orders[i];
            const auto& info = sort_key_infos[i++];
            // TODO: use custom comparison logic instead of memcmp for special types like strings
            cmp = memcmp(reinterpret_cast<const char*>(a.data) + info.offset, reinterpret_cast<const char*>(b.data) + info.offset, info.column->getValueTypeSize());
            cmp *= order == Order::Ascending ? 1 : -1;
        }
        return cmp;
    };
}

void swap(Row a, Row b) {
    assert(a.size == b.size);
    char* row_a = reinterpret_cast<char*>(a.data);
    char* row_b = reinterpret_cast<char*>(b.data);
    std::swap_ranges(row_a, row_a + a.size, row_b);
}

template<class It, class Compare>
It quicksortPartition(It first, It last, Compare comp) {
    It pivot = first;
    typename It::difference_type count = 0;
    for (It it = first + 1; it < last; it++) {
        if (comp(*it, *pivot) < 0)
            count++;
    }
    if (count == 0)
        return first;
    swap(*pivot, *(first + count));
    pivot = first + count;

    // sort left and right of the pivot element
    It i = first, j = last - 1;
    while (i < pivot && j > pivot) {
        while (comp(*i, *pivot) < 0) {
            i++;
        }
        while (comp(*j, *pivot) >= 0 && j > pivot) {
            j--;
        }
        if (i < pivot && j > pivot) {
            swap(*i++, *j--);
        }
    }

    return first + count;
}

template<class It, class Compare>
void quicksort(It first, It last, Compare comp) {
    if (first >= last)
        return;

    It p = quicksortPartition(first, last, comp);
    quicksort(first, p, comp);
    quicksort(p + 1, last, comp);

#ifndef NDEBUG
    for (It it = first; it < last - 1; it++)
        assert(comp(*it, *(it + 1)) <= 0);
#endif
}

void SortBreaker::push(std::shared_ptr<Batch> batch, uint32_t worker_id) {
    if (batch->getRowSize() != batch_description.getRowSize())
        throw std::runtime_error("SortBreaker: Batch row size does not match batch_description");
    quicksort(batch->begin(), batch->end(), comp);
    batches.at(worker_id).push_back(batch);
    valid_row_count += batch->getValidRowCount();
}

void SortOperator::execute(size_t, size_t, uint32_t worker_id) {
    // TODO: implement parallel multiway mergesort to parallelize this
    const size_t row_size = breaker->batch_description.getRowSize();
    std::shared_ptr<Batch> intermediates = std::make_shared<Batch>(vmcache, row_size, worker_id);

    for (size_t i = 0; i < breaker->getValidRowCount(); i++) {
        uint32_t row_id;
        char* loc = reinterpret_cast<char*>(intermediates->addRowIfPossible(row_id));
        if (loc == nullptr) {
            next_operator->push(intermediates, worker_id);
            if (intermediates.use_count() > 1) {
                intermediates = std::make_shared<Batch>(vmcache, row_size, worker_id);
            } else {
                intermediates->clear();
            }
            loc = reinterpret_cast<char*>(intermediates->addRowIfPossible(row_id));
        }
        // find next row in the pre-sorted batches
        Batch::Iterator candidate = intermediates->end();
        size_t candidate_batch_i = 0;
        for (size_t j = 0; j < batches.size(); j++) {
            if (!batches[j]->empty()) {
                auto b_begin = batches[j]->begin();
                if (!candidate || breaker->comp(*b_begin, *candidate) < 0) {
                    candidate = b_begin;
                    candidate_batch_i = j;
                }
            }
        }
        // TODO: it could be more concise to just implement markInvalid() in Batch::Iterator
        batches[candidate_batch_i]->markInvalid(candidate);
        memcpy(loc, (*candidate).data, row_size);
    }

    if (intermediates->getCurrentSize() > 0)
        next_operator->push(intermediates, worker_id);
}