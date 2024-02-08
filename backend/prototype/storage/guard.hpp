#pragma once

#include <immintrin.h>

#include "vmcache.hpp"

struct OLRestartException {};

template<class T>
struct OptimisticGuard {
    VMCache& vmcache;
    uint32_t worker_id;
    PageId pid;
    T* data;
    uint64_t version; // note: this "version" includes the page state and modified/dirty bits
    static const PageId MOVED = ~0ull;

    explicit OptimisticGuard(VMCache& vmcache, PageId pid, uint32_t worker_id) : vmcache(vmcache), worker_id(worker_id), pid(pid), data(reinterpret_cast<T*>(vmcache.toPointer(pid))) {
        init();
    }

    template<class T2>
    OptimisticGuard(PageId pid, OptimisticGuard<T2>& parent) : vmcache(parent.vmcache), worker_id(parent.worker_id), pid(pid) {
        parent.checkVersionAndRestart();
        data = reinterpret_cast<T*>(vmcache.toPointer(pid));
        init();
    }

    ~OptimisticGuard() noexcept(false) {
        checkVersionAndRestart();
    }

    OptimisticGuard(OptimisticGuard&& other) : vmcache(other.vmcache) { // move
        worker_id = other.worker_id;
        pid = other.pid;
        data = other.data;
        version = other.version;
    }

    OptimisticGuard& operator=(OptimisticGuard&& other) { // move
        if (pid != MOVED)
            checkVersionAndRestart();
        assert(worker_id == other.worker_id);
        pid = other.pid;
        data = other.data;
        version = other.version;
        other.pid = MOVED;
        other.data = nullptr;
        return *this;
    }

    OptimisticGuard& operator=(const OptimisticGuard& other) { // copy
        release();
        return *this = OptimisticGuard(other);
    }

    OptimisticGuard(const OptimisticGuard& other) // copy
        : vmcache(other.vmcache)
        , worker_id(other.worker_id)
        , pid(other.pid)
        , data(other.data)
        , version(other.version) { }

    void init() {
        assert(pid != MOVED);
        PageState& ps = vmcache.getPageState(pid);
        for (size_t repeat_counter = 0; ; repeat_counter++) {
            uint64_t state = ps.load();
            switch (PAGE_STATE(state)) {
                case PAGE_STATE_MARKED: {
                    uint64_t new_state = (state & ~PAGE_STATE_MASK) | PAGE_STATE_UNLOCKED;
                    if (ps.compare_exchange_weak(state, new_state)) {
                        version = new_state;
                        return;
                    }
                    break;
                }
                case PAGE_STATE_LOCKED:
                    break;
                case PAGE_STATE_EVICTED: {
                    uint64_t new_state = (state & ~PAGE_STATE_MASK) | PAGE_STATE_LOCKED;
                    if (ps.compare_exchange_strong(state, new_state)) {
                        vmcache.fault(pid, PAGE_MODIFIED(state), false, worker_id);
                        ps.store((state & ~PAGE_STATE_MASK) | PAGE_STATE_UNLOCKED, std::memory_order_release);
                    }
                    break;
                }
                default:
                    version = state;
                    return;
            }
            _mm_pause(); // yield
        }
    }

    void checkVersionAndRestart() {
        if (pid != MOVED) {
            PageState& ps = vmcache.getPageState(pid);
            uint64_t state = ps.load();
            if (version == state) // fast path, nothing changed
                return;
            if (PAGE_VERSION(version) == PAGE_VERSION(state)) { // same version
                uint64_t s = PAGE_STATE(state);
                if (s <= PAGE_STATE_LOCKED_SHARED_MAX)
                    return; // ignore shared locks
                if (s == PAGE_STATE_MARKED) {
                    uint64_t new_state = (state & ~PAGE_STATE_MASK) | PAGE_STATE_UNLOCKED;
                    if (ps.compare_exchange_weak(state, new_state))
                        return; // mark cleared
                }
            }
            // invalidate
            pid = MOVED;
            data = nullptr;
            if (std::uncaught_exceptions() == 0)
                throw OLRestartException();
        }
   }

    T* operator->() const {
        assert(pid != MOVED);
        return data;
    }

    void release() {
        checkVersionAndRestart();
        pid = MOVED;
        data = nullptr;
    }

    bool isReleased() const {
        return pid == MOVED;
    }
};

template<class T>
struct SharedGuard {
    VMCache& vmcache;
    uint32_t worker_id;
    PageId pid;
    T* data;
    static const PageId MOVED = ~0ull;

    SharedGuard(VMCache& vmcache, uint32_t worker_id): vmcache(vmcache), worker_id(worker_id), pid(MOVED), data(nullptr) {}

    explicit SharedGuard(VMCache& vmcache, PageId pid, uint32_t worker_id) : vmcache(vmcache), worker_id(worker_id), pid(pid) {
        data = reinterpret_cast<T*>(vmcache.fixShared(pid, worker_id));
    }

    ~SharedGuard() {
        if (pid != MOVED)
            vmcache.unfixShared(pid);
    }

    SharedGuard& operator=(const SharedGuard& other) { // copy
        if (pid != MOVED)
            vmcache.unfixShared(pid);
        worker_id = other.worker_id;
        pid = other.pid;
        data = reinterpret_cast<T*>(vmcache.fixShared(pid, worker_id));
        return *this;
    }

    SharedGuard(const SharedGuard& other) : vmcache(other.vmcache) { // copy
        worker_id = other.worker_id;
        pid = other.pid;
        data = reinterpret_cast<T*>(vmcache.fixShared(pid, worker_id));
    }

    SharedGuard& operator=(SharedGuard&& other) { // move
        if (pid != MOVED)
            vmcache.unfixShared(pid);
        worker_id = other.worker_id;
        pid = other.pid;
        data = other.data;
        other.pid = MOVED;
        other.data = nullptr;
        return *this;
    }

    SharedGuard(SharedGuard&& other) : vmcache(other.vmcache) { // move
        worker_id = other.worker_id;
        pid = other.pid;
        data = other.data;
        other.pid = MOVED;
        other.data = nullptr;
    }

    const T* operator->() const {
        assert(pid != MOVED);
        return data;
    }

    void release() {
        if (pid != MOVED) {
            vmcache.unfixShared(pid);
            pid = MOVED;
        }
    }

    bool isReleased() const {
        return pid == MOVED;
    }
};

template<class T>
struct ExclusiveGuard {
    VMCache& vmcache;
    PageId pid;
    T* data;
    static const PageId MOVED = ~0ull;

    ExclusiveGuard(VMCache& vmcache): vmcache(vmcache), pid(MOVED), data(nullptr) {}

    explicit ExclusiveGuard(VMCache& vmcache, PageId pid, uint32_t worker_id) : vmcache(vmcache), pid(pid) {
        data = reinterpret_cast<T*>(vmcache.fixExclusive(pid, worker_id));
    }

    explicit ExclusiveGuard(OptimisticGuard<T>&& other) : vmcache(other.vmcache), pid(MOVED), data(nullptr) {
        assert(other.pid != MOVED);
        for (size_t repeat_counter = 0; ; repeat_counter++) {
            PageState& ps = vmcache.getPageState(other.pid);
            uint64_t state = ps.load();
            if (PAGE_VERSION(state) != PAGE_VERSION(other.version))
                throw OLRestartException();
            uint64_t s = PAGE_STATE(state);
            if (s == PAGE_STATE_UNLOCKED || s == PAGE_STATE_MARKED) {
                if (ps.compare_exchange_strong(state, (state & ~PAGE_STATE_MASK) | PAGE_STATE_LOCKED)) {
                    // note: there is no need to call VMCache::fault() here, as we are upgrading from an optimistic latch, which will have already faulted the page
                    pid = other.pid;
                    data = other.data;
                    other.pid = MOVED;
                    other.data = nullptr;
                    return;
                }
            }
        }
    }

    ExclusiveGuard(const ExclusiveGuard&) = delete; // copy
    ExclusiveGuard& operator=(const ExclusiveGuard&) = delete; // copy

    ExclusiveGuard& operator=(ExclusiveGuard&& other) { // move
        if (pid != MOVED) {
            vmcache.unfixExclusive(pid);
        }
        pid = other.pid;
        data = other.data;
        other.pid = MOVED;
        other.data = nullptr;
        return *this;
    }

    ExclusiveGuard(ExclusiveGuard&& other) : vmcache(other.vmcache) { // move
        pid = other.pid;
        data = other.data;
        other.pid = MOVED;
        other.data = nullptr;
    }

    ~ExclusiveGuard() {
        if (pid != MOVED)
            vmcache.unfixExclusive(pid);
    }

    T* operator->() {
        assert(pid != MOVED);
        return data;
    }

    void release() {
        if (pid != MOVED) {
            vmcache.unfixExclusive(pid);
            pid = MOVED;
        }
    }

    bool isReleased() const {
        return pid == MOVED;
    }
};

template<class T>
struct AllocGuard : public ExclusiveGuard<T> {
    template <typename ...Params>
    AllocGuard(VMCache& vmcache, uint32_t worker_id, Params&&... params) : ExclusiveGuard<T>(vmcache) {
        ExclusiveGuard<T>::pid = vmcache.allocatePage();
        ExclusiveGuard<T>::data = reinterpret_cast<T*>(vmcache.fixExclusive(ExclusiveGuard<T>::pid, worker_id));
        new (ExclusiveGuard<T>::data) T(std::forward<Params>(params)...);
    }
};