#pragma once

#include <cassert>

#include "../storage/persistence/column.hpp"
#include "../storage/guard.hpp"
#include "../storage/vmcache.hpp"

struct GeneralPagedVectorIterator {
    static constexpr size_t UNLOAD = std::numeric_limits<size_t>::max();

    GeneralPagedVectorIterator(VMCache& vmcache, PageId basepage, size_t i, size_t value_size, uint32_t worker_id)
        : vmcache(vmcache)
        , basepage_pid(basepage)
        , page(nullptr)
        , current_page_pid(0)
        , current_page_exclusive(false)
        , basepage(vmcache, basepage, worker_id)
        , basepage_num(0)
        , values_per_page(PAGE_SIZE / value_size)
        , page_num(i / values_per_page)
        , i(i % values_per_page)
        , value_size(value_size)
        , worker_id(worker_id)
    {
        if (i != UNLOAD)
            loadPage(false);
    }

    ~GeneralPagedVectorIterator() {
        try {
            basepage.release();
        } catch (const OLRestartException&) { } // do not care about validation failures on the basepage at this point
        release();
    }

    GeneralPagedVectorIterator(const GeneralPagedVectorIterator& other) // copy
        : vmcache(other.vmcache)
        , basepage_pid(other.basepage_pid)
        , page(other.page)
        , current_page_pid(other.current_page_pid)
        , current_page_exclusive(other.current_page_exclusive)
        , basepage(other.basepage)
        , basepage_num(other.basepage_num)
        , values_per_page(other.values_per_page)
        , page_num(other.page_num)
        , i(other.i)
        , value_size(other.value_size)
        , worker_id(other.worker_id)
    {
        // acquire shared latches for this new iterator instance
        if (page != nullptr) {
            vmcache.fixShared(current_page_pid, worker_id, true);
        }
    }

    GeneralPagedVectorIterator& operator=(const GeneralPagedVectorIterator& other) { // copy
        release();
        return *this = GeneralPagedVectorIterator(other);
    }

    GeneralPagedVectorIterator(GeneralPagedVectorIterator&& other) noexcept // move
        : vmcache(other.vmcache)
        , basepage_pid(other.basepage_pid)
        , page(other.page)
        , current_page_pid(other.current_page_pid)
        , current_page_exclusive(other.current_page_exclusive)
        , basepage(other.basepage)
        , basepage_num(other.basepage_num)
        , values_per_page(other.values_per_page)
        , page_num(other.page_num)
        , i(other.i)
        , value_size(other.value_size)
        , worker_id(other.worker_id)
    {
        // shared latches are now held by this instance
        other.page = nullptr;
    }

    GeneralPagedVectorIterator& operator=(GeneralPagedVectorIterator&& other) noexcept { // move
        release();
        basepage_pid = other.basepage_pid;
        page = other.page;
        current_page_pid = other.current_page_pid;
        current_page_exclusive = other.current_page_exclusive;
        basepage = std::move(other.basepage);
        basepage_num = other.basepage_num;
        values_per_page = other.values_per_page;
        page_num = other.page_num;
        i = other.i;
        value_size = other.value_size;
        worker_id = other.worker_id;
        // shared latches are now held by this instance
        other.page = nullptr;
        return *this;
    }

    inline void reposition(size_t idx, bool for_write = false) {
        size_t new_page_num = idx / values_per_page;
        i = idx % values_per_page;
        if (__builtin_expect(new_page_num != page_num || (for_write && !current_page_exclusive) || page == nullptr, false)) {
            page_num = new_page_num;
            loadPage(for_write);
        }
    }

    GeneralPagedVectorIterator& operator++() {
        if (__builtin_expect(i == UNLOAD, false))
            throw std::runtime_error("Invalid GeneralPagedVectorIterator incremented");
        i++;
        if (__builtin_expect((i == values_per_page), false)) {
            i = 0;
            page_num++;
            loadPage(false);
        }
        return *this;
    }

    const void* getCurrentValue() const {
        return page + value_size * i;
    }

    void* getCurrentValueForUpdate() {
        if (!current_page_exclusive) {
            vmcache.unfixShared(current_page_pid);
            vmcache.fixExclusive(current_page_pid, worker_id);
            current_page_exclusive = true;
        }
        return page + value_size * i;
    }

    inline void release() {
        if (page != nullptr) {
            unfixCurrentPage();
        }
    }

protected:
    VMCache& vmcache;
    PageId basepage_pid;
    char* page;
    PageId current_page_pid;
    bool current_page_exclusive; // true if we are holding an exclusive lock on the current page, false if only holding a shared lock
    OptimisticGuard<ColumnBasepage> basepage;
    size_t basepage_num;
    size_t values_per_page;
    size_t page_num;
    size_t i;
    size_t value_size;
    uint32_t worker_id;

    inline void unfixCurrentPage() {
        if (current_page_exclusive)
            vmcache.unfixExclusive(current_page_pid);
        else
            vmcache.unfixShared(current_page_pid);
        page = nullptr;
    }

    inline void loadPage(bool for_write) {
        // resolve required basepage first to get pid
        const size_t data_pages_per_basepage = (PAGE_SIZE - sizeof(ColumnBasepage)) / sizeof(PageId);
        const size_t req_basepage_num = page_num / data_pages_per_basepage;
        const size_t off_in_basepage = page_num % data_pages_per_basepage;
        for (size_t restart_counter = 0; ; restart_counter++) {
            try {
                while (basepage_num != req_basepage_num || basepage.isReleased()) {
                    if (basepage.isReleased() || basepage_num > req_basepage_num) {
                        basepage = OptimisticGuard<ColumnBasepage>(vmcache, basepage_pid, worker_id);
                        basepage_num = 0;
                    } else {
                        basepage = OptimisticGuard<ColumnBasepage>(basepage->next, basepage);
                        basepage_num++;
                    }
                }

                // resolve data page
                if (__builtin_expect((page != nullptr), 1)) {
                    unfixCurrentPage();
                }
                current_page_pid = basepage->data_pages[off_in_basepage];
                basepage.checkVersionAndRestart();
                break;
            } catch (const OLRestartException&) { }
        }

        page = for_write ? vmcache.fixExclusive(current_page_pid, worker_id) : vmcache.fixShared(current_page_pid, worker_id, true);
        current_page_exclusive = for_write;
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

    const reference operator*() const { return reinterpret_cast<T*>(page)[i]; }

    friend bool operator== (const PagedVectorIterator<T>& a, const PagedVectorIterator<T>& b) { return &a.pids == &b.pids && a.i == b.i; };

    friend bool operator!= (const PagedVectorIterator<T>& a, const PagedVectorIterator<T>& b) { return &a.pids != &b.pids || a.i != b.i; };
};