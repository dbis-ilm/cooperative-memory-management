#include <gtest/gtest.h>

#include "prototype/utils/hashset.hpp"

TEST(HashSet, erase) {
    HashSet<uint64_t> set(1024);
    EXPECT_EQ(set.erase(1), 0);

    set.insert(1);
    EXPECT_EQ(set.erase(1), 1);
    EXPECT_EQ(set.erase(1), 0);
}