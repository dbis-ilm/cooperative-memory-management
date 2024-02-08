#include <gtest/gtest.h>

#include <fcntl.h>
#include <sys/file.h>

#include "prototype/storage/policy/basic_partitioning_strategy.hpp"
#include "prototype/storage/policy/cache_partition.hpp"
#include "prototype/storage/guard.hpp"
#include "prototype/storage/vmcache.hpp"

#define MAX_PHYSICAL_PAGES 4

class VMCacheFixture : public ::testing::Test {
public:
    std::string path;
    std::string lock_path;
    std::shared_ptr<VMCache> cache;

protected:
    void SetUp() override {
        path = "vmcache_test.db";
        lock_path = "vmcache_test.lock";
        lock_fd = open(lock_path.c_str(), O_CREAT);
        while (flock(lock_fd, LOCK_EX) != 0) {
            close(lock_fd);
            lock_fd = open(lock_path.c_str(), O_CREAT);
        }
        // delete pre-existing test database
        struct stat st;
        if (stat(path.c_str(), &st) == 0) {
            if (unlink(path.c_str()) != 0)
                throw std::runtime_error("Failed to delete existing vmcache test database");
        }
        // this configuration results in a limit of MAX_PHYSICAL_PAGES physical pages
        cache = std::make_shared<VMCache>((MAX_PHYSICAL_PAGES + 1) * PAGE_SIZE, 128, path, false, false, false, false, createPartitioningStrategy<BasicPartitioningStrategy>("clock"), false, false, 1);
    }

    void TearDown() override {
        cache = nullptr;

        unlink(path.c_str());
        unlink(lock_path.c_str());
        flock(lock_fd, LOCK_UN);
        close(lock_fd);
    }

private:
    int lock_fd;
};

TEST_F(VMCacheFixture, setup) {
    // this is a prerequisite for some of the other tests
    ASSERT_EQ(cache->getMaxPhysicalPages(), MAX_PHYSICAL_PAGES);
}

#define TEST_MAGIC 0xDEADBEEFDEADBEEFull
TEST_F(VMCacheFixture, persist) {
    PageId pid = cache->allocatePage();
    uint64_t* page = reinterpret_cast<uint64_t*>(cache->fixExclusive(pid, 0));
    for (size_t i = 0; i < PAGE_SIZE / sizeof(uint64_t); i++)
        page[i] = TEST_MAGIC;
    cache->unfixExclusive(pid);
    EXPECT_EQ(cache->getDirtyPageCount(), 1);
    EXPECT_EQ(cache->getTotalAccessedPageCount(), 1);

    // evict the page by filling up the cache with other pages
    std::vector<PageId> pids;
    for (size_t i = 0; i < MAX_PHYSICAL_PAGES; i++) {
        pids.push_back(cache->allocatePage());
        cache->fixShared(pids[i], 0);
    }
    EXPECT_EQ(cache->getTotalAccessedPageCount(), MAX_PHYSICAL_PAGES + 1);
    ASSERT_EQ(PAGE_STATE(cache->getPageState(pid).load()), PAGE_STATE_EVICTED);
    EXPECT_EQ(cache->getDirtyPageCount(), 0);
    ASSERT_TRUE(PAGE_MODIFIED(cache->getPageState(pid).load()));
    for (size_t i = 0; i < MAX_PHYSICAL_PAGES; i++) {
        cache->unfixShared(pids[i]);
    }

    // check if we can still read the page contents after faulting the page again
    EXPECT_EQ(cache->getTotalFaultedPageCount(), 0);
    page = reinterpret_cast<uint64_t*>(cache->fixShared(pid, 0));
    EXPECT_EQ(cache->getTotalFaultedPageCount(), 1);
    EXPECT_EQ(cache->getTotalAccessedPageCount(), MAX_PHYSICAL_PAGES + 2);
    for (size_t i = 0; i < PAGE_SIZE / sizeof(uint64_t); i++) {
        ASSERT_EQ(page[i], TEST_MAGIC);
    }
    cache->unfixShared(pid);
}

TEST_F(VMCacheFixture, sandbox) {
    PageId pid = cache->allocatePage();
    uint64_t* page = reinterpret_cast<uint64_t*>(cache->fixExclusive(pid, 0));
    for (size_t i = 0; i < PAGE_SIZE / sizeof(uint64_t); i++)
        page[i] = TEST_MAGIC;
    cache->unfixExclusive(pid);

    // re-initialize VMCache, this time in sandbox mode
    cache = nullptr;
    cache = std::make_shared<VMCache>((MAX_PHYSICAL_PAGES + 1) * PAGE_SIZE, 128, path, true, false, false, false, createPartitioningStrategy<BasicPartitioningStrategy>("clock"), false, false, 1);
    page = reinterpret_cast<uint64_t*>(cache->fixExclusive(pid, 0));
    // make sure that the page was persisted when we were not in sandbox mode
    for (size_t i = 0; i < PAGE_SIZE / sizeof(uint64_t); i++)
        ASSERT_EQ(page[i], TEST_MAGIC);
    // zero out the page in sandbox mode
    for (size_t i = 0; i < PAGE_SIZE / sizeof(uint64_t); i++)
        page[i] = 0ul;
    cache->unfixExclusive(pid);

    // re-initialize VMCache again, the change made previously (zeroing out the page) should not have been persisted
    cache = nullptr;
    cache = std::make_shared<VMCache>((MAX_PHYSICAL_PAGES + 1) * PAGE_SIZE, 128, path, false, false, false, false, createPartitioningStrategy<BasicPartitioningStrategy>("clock"), false, false, 1);
    page = reinterpret_cast<uint64_t*>(cache->fixShared(pid, 0));
    for (size_t i = 0; i < PAGE_SIZE / sizeof(uint64_t); i++)
        ASSERT_EQ(page[i], TEST_MAGIC);
    cache->unfixShared(pid);
}

struct DummyPage {
    uint8_t data[PAGE_SIZE];
};

TEST_F(VMCacheFixture, OptimisticGuard) {
    PageId pid = cache->allocatePage();
    {
        // page should be faulted
        OptimisticGuard<DummyPage> guard(*cache, pid, 0);
        ASSERT_NO_THROW(guard.checkVersionAndRestart());
        // nothing changed
        ASSERT_NO_THROW(guard.checkVersionAndRestart());
    }
    {
        // concurrent shared latch should not cause a validation failure
        OptimisticGuard<DummyPage> guard(*cache, pid, 0);
        cache->fixShared(pid, 0);
        ASSERT_NO_THROW(guard.checkVersionAndRestart());
        cache->unfixShared(pid);
        // .. also not if the shared latch was already held before acquiring the optimistic one
        cache->fixShared(pid, 0);
        guard.init();
        ASSERT_NO_THROW(guard.checkVersionAndRestart());
        cache->unfixShared(pid);
    }
    {
        // validation failure due to concurrent (still active) write
        OptimisticGuard<DummyPage> guard(*cache, pid, 0);
        cache->fixExclusive(pid, 0);
        ASSERT_THROW(guard.checkVersionAndRestart(), OLRestartException);
        cache->unfixExclusive(pid);
    }
    {
        // validation failure due to concurrent write
        OptimisticGuard<DummyPage> guard(*cache, pid, 0);
        cache->fixExclusive(pid, 0);
        cache->unfixExclusive(pid);
        ASSERT_THROW(guard.checkVersionAndRestart(), OLRestartException);
    }
}