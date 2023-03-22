#pragma once

#include <cassert>

#include "../storage/persistence/column.hpp"
#include "../storage/vmcache.hpp"

struct GeneralPagedVectorIterator {
    GeneralPagedVectorIterator(VMCache& vmcache, PageId basepage, size_t i, size_t value_size, uint32_t worker_id)
        : vmcache(vmcache)
        , basepage_pid(basepage)
        , page(nullptr)
        , current_page_pid(0)
        , basepage(nullptr)
        , current_basepage_pid(0)
        , basepage_num(0)
        , values_per_page(PAGE_SIZE / value_size)
        , page_num(i / values_per_page)
        , i(i % values_per_page)
        , value_size(value_size)
        , worker_id(worker_id)
    {
        loadPage();
    }

    ~GeneralPagedVectorIterator() {
        if (basepage != nullptr) {
            vmcache.unfixShared(current_basepage_pid);
        }
        if (page != nullptr) {
            vmcache.unfixShared(current_page_pid);
        }
    }

    GeneralPagedVectorIterator(const GeneralPagedVectorIterator& other) // copy
        : vmcache(other.vmcache)
        , basepage_pid(other.basepage_pid)
        , page(other.page)
        , current_page_pid(other.current_page_pid)
        , basepage(other.basepage)
        , current_basepage_pid(other.current_basepage_pid)
        , basepage_num(other.basepage_num)
        , values_per_page(other.values_per_page)
        , page_num(other.page_num)
        , i(other.i)
        , value_size(other.value_size)
        , worker_id(other.worker_id)
    {
        // acquire shared latches for this new iterator instance
        if (basepage != nullptr) {
            vmcache.fixShared(current_basepage_pid, worker_id);
        }
        if (page != nullptr) {
            vmcache.fixShared(current_page_pid, worker_id, true);
        }
    }

    GeneralPagedVectorIterator& operator=(const GeneralPagedVectorIterator& other) { // copy
        if (basepage != nullptr) {
            vmcache.unfixShared(current_basepage_pid);
        }
        if (page != nullptr) {
            vmcache.unfixShared(current_page_pid);
        }
        return *this = GeneralPagedVectorIterator(other);
    }

    GeneralPagedVectorIterator(GeneralPagedVectorIterator&& other) noexcept // move
        : vmcache(other.vmcache)
        , basepage_pid(other.basepage_pid)
        , page(other.page)
        , current_page_pid(other.current_page_pid)
        , basepage(other.basepage)
        , current_basepage_pid(other.current_basepage_pid)
        , basepage_num(other.basepage_num)
        , values_per_page(other.values_per_page)
        , page_num(other.page_num)
        , i(other.i)
        , value_size(other.value_size)
        , worker_id(other.worker_id)
    {
        // shared latches are now held by this instance
        other.basepage = nullptr;
        other.page = nullptr;
    }

    GeneralPagedVectorIterator& operator=(GeneralPagedVectorIterator&& other) noexcept { // move
        if (basepage != nullptr) {
            vmcache.unfixShared(current_basepage_pid);
        }
        if (page != nullptr) {
            vmcache.unfixShared(current_page_pid);
        }
        basepage_pid = other.basepage_pid;
        page = other.page;
        current_page_pid = other.current_page_pid;
        basepage = other.basepage;
        current_basepage_pid = other.current_basepage_pid;
        basepage_num = other.basepage_num;
        values_per_page = other.values_per_page;
        page_num = other.page_num;
        i = other.i;
        value_size = other.value_size;
        worker_id = other.worker_id;
        // shared latches are now held by this instance
        other.basepage = nullptr;
        other.page = nullptr;
        return *this;
    }

    inline void reposition(size_t idx) {
        size_t new_page_num = idx / values_per_page;
        i = idx % values_per_page;
        if (new_page_num != page_num) {
            page_num = new_page_num;
            loadPage();
        }
    }

    GeneralPagedVectorIterator& operator++() {
        i++;
        if (__builtin_expect((i == values_per_page), 0)) {
            i = 0;
            page_num++;
            loadPage();
        }
        return *this;
    }

    void* getCurrentValue() const {
        return page + value_size * i;
    }

protected:
    VMCache& vmcache;
    PageId basepage_pid;
    char* page;
    PageId current_page_pid;
    ColumnBasepage* basepage;
    PageId current_basepage_pid;
    size_t basepage_num;
    size_t values_per_page;
    size_t page_num;
    size_t i;
    size_t value_size;
    uint32_t worker_id;

    inline void loadPage() {
        // resolve required basepage first to get pid
        const size_t data_pages_per_basepage = (PAGE_SIZE - sizeof(ColumnBasepage)) / sizeof(PageId);
        const size_t req_basepage_num = page_num / data_pages_per_basepage;
        const size_t off_in_basepage = page_num % data_pages_per_basepage;
        while (basepage_num != req_basepage_num || basepage == nullptr) {
            size_t pid = 0;
            if (basepage == nullptr || basepage_num > req_basepage_num) {
                pid = basepage_pid;
                basepage_num = 0;
            } else {
                pid = basepage->next;
                basepage_num++;
            }
            if (__builtin_expect((basepage != nullptr), 1))
                vmcache.unfixShared(current_basepage_pid);
            basepage = reinterpret_cast<ColumnBasepage*>(vmcache.fixShared(pid, worker_id));
            current_basepage_pid = pid;
        }

        // resolve data page
        if (__builtin_expect((page != nullptr), 1))
            vmcache.unfixShared(current_page_pid);
        page = vmcache.fixShared(basepage->data_pages[off_in_basepage], worker_id, true);
        current_page_pid = basepage->data_pages[off_in_basepage];
    }
};

template <typename T>
struct PagedVectorIterator : GeneralPagedVectorIterator {
    using iterator_category = std::input_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = T;
    using pointer = T*;
    using reference = T&;

    PagedVectorIterator(VMCache& vmcache, PageId basepage, size_t i, uint32_t worker_id)
        : GeneralPagedVectorIterator(vmcache, basepage, i, sizeof(T), worker_id) { }

    reference operator*() const { return reinterpret_cast<T*>(page)[i]; }

    friend bool operator== (const PagedVectorIterator<T>& a, const PagedVectorIterator<T>& b) { return &a.pids == &b.pids && a.i == b.i; };

    friend bool operator!= (const PagedVectorIterator<T>& a, const PagedVectorIterator<T>& b) { return &a.pids != &b.pids || a.i != b.i; };
};