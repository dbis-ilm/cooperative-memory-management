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
            cmp = info.column->cmp(reinterpret_cast<const char*>(a.data) + info.offset, reinterpret_cast<const char*>(b.data) + info.offset);
            cmp *= order == Order::Ascending ? 1 : -1;
        }
        return cmp;
    };
}

SortBreaker::SortBreaker(BatchDescription& batch_description, std::function<int(const Row&, const Row&)>&& comp, size_t num_workers)
: DefaultBreaker(batch_description, num_workers)
, comp(comp) { }

void swap(Row a, Row b) {
    assert(a.size == b.size);
    char* row_a = reinterpret_cast<char*>(a.data);
    char* row_b = reinterpret_cast<char*>(b.data);
    std::swap_ranges(row_a, row_a + a.size, row_b);
}

template<class It, class Compare>
It selectPivot(It first, It last, Compare comp) {
    It mid = first + (last - first) / 2;
    if (comp(*last, *first) < 0)
        swap(*first, *last);
    if (comp(*mid, *first) < 0)
        swap(*first, *mid);
    if (comp(*last, *mid) < 0)
        swap(*mid, *last);
    return mid;
}

template<class It, class Compare>
It quicksortPartition(It begin, It end, Compare comp) {
    It pivot = selectPivot(begin, end - 1, comp);
    typename It::difference_type count = 0;
    for (It it = begin; it < end; ++it) {
        if (comp(*it, *pivot) < 0)
            count++;
    }
    if (count != pivot - begin) {
        swap(*pivot, *(begin + count));
        pivot = begin + count;
    }

    // sort left and right of the pivot element
    It i = begin, j = end - 1;
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

#ifndef NDEBUG
    for (It it = begin; it < pivot; ++it) {
        assert(comp(*it, *pivot) < 0);
    }
    for (It it = pivot + 1; it < end; ++it) {
        assert(comp(*it, *pivot) >= 0);
    }
#endif

    return pivot;
}

template<class It, class Compare>
void insertionsort(It begin, It end, Compare comp) {
    It i = begin + 1;
    while (i != end) {
        It j = i;
        while (j != begin && comp(*(j - 1), *j) > 0) {
            swap(*j, *(j - 1));
            --j;
        }
        ++i;
    }

#ifndef NDEBUG
    for (It it = begin; it < end - 1; ++it) {
        assert(comp(*it, *(it + 1)) <= 0);
    }
#endif
}

template<class It, class Compare>
void heapify(It begin, It end, Compare comp) {
    auto start = end - 1;

    while (start > begin) {
        --start;
        siftDown(begin, end, start, comp);
    }
}

template<class It, class Compare>
void siftDown(It begin, It end, It root, Compare comp) {
    while (begin + (root - begin) * 2 + 1 < end) {
        auto child = begin + (root - begin) * 2 + 1;
        if (child + 1 < end && comp(*child, *(child + 1)) < 0)
            child = child + 1;

        if (comp(*root, *child) < 0) {
            swap(*root, *child);
            root = child;
        } else {
            return;
        }
    }
}

template<class It, class Compare>
void heapsort(It begin, It end, Compare comp) {
    heapify(begin, end, comp);
    while (end != begin + 1) {
        --end;
        swap(*begin, *end);
        siftDown(begin, end, begin, comp);
    }

#ifndef NDEBUG
    for (It it = begin; it < end - 1; ++it) {
        assert(comp(*it, *(it + 1)) <= 0);
    }
#endif
}

template<class It, class Compare>
void introsort(It begin, It end, Compare comp, size_t maxdepth) {
    if (begin >= end)
        return;

    const typename It::difference_type n = end - begin;
    if (n < 16) {
        insertionsort(begin, end, comp);
    } else if (maxdepth == 0) {
        heapsort(begin, end, comp);
    } else {
        It p = quicksortPartition(begin, end, comp);
        introsort(begin, p, comp, maxdepth - 1);
        introsort(p + 1, end, comp, maxdepth - 1);
#ifndef NDEBUG
        for (It it = begin; it < end - 1; ++it) {
            assert(comp(*it, *(it + 1)) <= 0);
        }
#endif
    }
}

template<class It, class Compare>
void introsort(It begin, It end, Compare comp) {
    const typename It::difference_type n = end - begin;
    if (n == 0)
        return;
    size_t maxdepth = (8 * sizeof(unsigned long) - __builtin_clzl(n - 1)) * 2; // log2(n) * 2
    introsort(begin, end, comp, maxdepth);
}

void SortBreaker::push(std::shared_ptr<Batch> batch, uint32_t worker_id) {
    if (batch->getRowSize() != batch_description.getRowSize())
        throw std::runtime_error("SortBreaker: Batch row size does not match batch_description");
    valid_row_count += batch->getValidRowCount();
    if (batch->full()) {
        // immediately sort the batch and add it to the thread-local list of batches
        introsort(batch->begin(), batch->end(), comp);
        batches.at(worker_id).push_back(batch);
    } else {
        while (!batch->empty()) {
            if (batches.at(worker_id).empty() || batches.at(worker_id).back()->full()) {
                // add batch to the thread-local list of batches, but do not sort it yet
                batches.at(worker_id).push_back(batch);
                break;
            }
            // append batch contents to the last thread-local batch
            batches.at(worker_id).back()->append(batch);
            if (batches.at(worker_id).back()->full()) {
                introsort(batches.at(worker_id).back()->begin(), batches.at(worker_id).back()->end(), comp);
            }
        }
    }
}

void SortBreaker::consumeBatches(std::vector<std::shared_ptr<Batch>>& target, uint32_t) {
    if (!target.empty()) {
        throw std::runtime_error("Target not empty");
    }

    size_t batch_count = 0;
    for (const std::vector<std::shared_ptr<Batch>>& worker_batches : batches) {
        batch_count += worker_batches.size();
    }
    target.reserve(batch_count);
    for (std::vector<std::shared_ptr<Batch>>& worker_batches : batches) {
        for (std::shared_ptr<Batch>& batch : worker_batches) {
            if (!batch->full()) {
                // batch has not been pre-sorted yet, do this now
                introsort(batch->begin(), batch->end(), comp);
            }
            target.push_back(batch);
            batch = nullptr;
        }
    }
}


void SortOperator::execute(size_t, size_t, uint32_t worker_id) {
    // TODO: implement parallel multiway mergesort to parallelize this
    const size_t row_size = breaker->batch_description.getRowSize();
    IntermediateHelper intermediates(vmcache, row_size, next_operator, worker_id);
    for (size_t i = 0; i < breaker->getValidRowCount(); i++) {
        char* loc = intermediates.addRow();
        // find next row in the pre-sorted batches
        assert(batches.size() > 0);
        Batch::Iterator candidate = batches.front()->end();
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
}