#include <gtest/gtest.h>

#include "prototype/core/types.hpp"

TEST(CompositeKey, operator_gt) {
    auto key1 = CompositeKey<4> { 1, 1, 2981, 10 };
    auto key2 = CompositeKey<4> { 1, 1, 166, 1 };
    EXPECT_TRUE(key1 > key2);
    auto key3 = CompositeKey<4> { 2, 1, 166, 1 };
    EXPECT_TRUE(key3 > key2);
    auto key4 = CompositeKey<4> { 2, 2, 0, 0 };
    auto key5 = CompositeKey<4> { 2, 0, 0, 1 };
    EXPECT_TRUE(key4 > key5);
}

TEST(CompositeKey, operator_lt) {
    auto key1 = CompositeKey<4> { 1, 1, 2981, 10 };
    auto key2 = CompositeKey<4> { 1, 1, 166, 1 };
    EXPECT_FALSE(key1 < key2);
    auto key3 = CompositeKey<4> { 2, 1, 166, 1 };
    EXPECT_FALSE(key3 < key2);
    auto key4 = CompositeKey<4> { 2, 2, 0, 0 };
    auto key5 = CompositeKey<4> { 2, 0, 0, 1 };
    EXPECT_FALSE(key4 < key5);
}

TEST(CompositeKey, transitive_cmp) {
    auto key1 = CompositeKey<4> { 0, 0, 0, 0 };
    auto key2 = CompositeKey<4> { 0, 0, 0, 1 };
    auto key3 = CompositeKey<4> { 1, 0, 0, 0 };
    EXPECT_TRUE(key1 < key3);
    EXPECT_TRUE(key2 < key3);
    EXPECT_TRUE(key1 < key2);
    EXPECT_TRUE(key3 > key1);
    EXPECT_TRUE(key3 > key2);
    EXPECT_TRUE(key2 > key1);
}