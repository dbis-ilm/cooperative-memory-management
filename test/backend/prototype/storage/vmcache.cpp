#include <gtest/gtest.h>

#include <fcntl.h>

#include "prototype/storage/policy/basic_partitioning_strategy.hpp"
#include "prototype/storage/policy/cache_partition.hpp"
#include "prototype/storage/vmcache.hpp"

#define MAX_PHYSICAL_PAGES 4

class VMCacheFixture : public ::testing::Test {
public:
    int fd;
    std::shared_ptr<VMCache> cache;

protected:
    void SetUp() override {
        std::string path("vmcache_test.dat");
        int flags = O_RDWR | O_DIRECT | O_CREAT | O_TRUNC;
        fd = open(path.c_str(), flags, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
        if (fd == -1) {
            throw std::runtime_error("Could not open file for VMCache test");
        }
        // this configuration results in a limit of MAX_PHYSICAL_PAGES physical pages
        cache = std::make_shared<VMCache>((MAX_PHYSICAL_PAGES + 1) * PAGE_SIZE, 128, fd, createPartitioningStrategy<BasicPartitioningStrategy>("clock"), false, 1);
    }

    void TearDown() override {
        cache = nullptr;
        close(fd);
    }

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

    // evict the page by filling up the cache with other pages
    std::vector<PageId> pids;
    for (size_t i = 0; i < MAX_PHYSICAL_PAGES; i++) {
        pids.push_back(cache->allocatePage());
        cache->fixShared(pids[i], 0);
    }
    ASSERT_EQ(PAGE_STATE(cache->getPageState(pid).load()), PAGE_STATE_EVICTED);
    for (size_t i = 0; i < MAX_PHYSICAL_PAGES; i++) {
        cache->unfixShared(pids[i]);
    }

    // check if we can still read the page contents after faulting the page again
    page = reinterpret_cast<uint64_t*>(cache->fixShared(pid, 0));
    for (size_t i = 0; i < PAGE_SIZE / sizeof(uint64_t); i++)
        ASSERT_EQ(page[i], TEST_MAGIC);
    cache->unfixShared(pid);
}