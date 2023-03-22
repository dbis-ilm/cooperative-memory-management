#include "../../core/db_test.hpp"
#include "prototype/core/db.hpp"
#include "prototype/core/types.hpp"
#include "prototype/scheduling/execution_context.hpp"
#include "prototype/storage/persistence/btree.hpp"

TEST(BTree, lowerBound) {
    size_t array[] = { 1, 6, 9, 10, 12, 15 };
    EXPECT_EQ(lowerBound<size_t>(array, 6, 1), 0);
    EXPECT_EQ(lowerBound<size_t>(array, 6, 6), 1);
    EXPECT_EQ(lowerBound<size_t>(array, 6, 9), 2);
    EXPECT_EQ(lowerBound<size_t>(array, 6, 10), 3);
    EXPECT_EQ(lowerBound<size_t>(array, 6, 12), 4);
    EXPECT_EQ(lowerBound<size_t>(array, 6, 15), 5);

    EXPECT_EQ(lowerBound<size_t>(array, 6, 13), 5);
    EXPECT_EQ(lowerBound<size_t>(array, 6, 11), 4);

    size_t array2[] = { 1, 6, 9 };
    EXPECT_EQ(lowerBound<size_t>(array2, 3, 1), 0);
    EXPECT_EQ(lowerBound<size_t>(array2, 3, 6), 1);
    EXPECT_EQ(lowerBound<size_t>(array2, 3, 9), 2);
    EXPECT_EQ(lowerBound<size_t>(array2, 3, 12), 3);
    EXPECT_EQ(lowerBound<size_t>(array2, 3, 7), 2);
}

class BTreeFixture : public DBTestFixture { };

TEST_F(BTreeFixture, double_insert) {
    BTree<uint32_t, size_t> tree(db->vmcache, context->getWorkerId());
    tree.insert(1, 1);
    EXPECT_ANY_THROW(tree.insert(1, 2));
}

TEST_F(BTreeFixture, lookup) {
    BTree<uint32_t, size_t> tree(db->vmcache, context->getWorkerId());
    tree.insert(3, 1);
    tree.insert(21, 2);
    tree.insert(412, 3);
    tree.insert(1, 4);
    tree.insert(32, 5);
    tree.insert(2, 6);
    auto it = tree.lookup(3);
    EXPECT_NE(it, tree.end());
    EXPECT_EQ((*it).second, 1);
    it = tree.lookup(21);
    EXPECT_NE(it, tree.end());
    EXPECT_EQ((*it).second, 2);
    it = tree.lookup(412);
    EXPECT_NE(it, tree.end());
    EXPECT_EQ((*it).second, 3);
    it = tree.lookup(1);
    EXPECT_NE(it, tree.end());
    EXPECT_EQ((*it).second, 4);
    it = tree.lookup(32);
    EXPECT_NE(it, tree.end());
    EXPECT_EQ((*it).second, 5);
    it = tree.lookup(2);
    EXPECT_NE(it, tree.end());
    EXPECT_EQ((*it).second, 6);
    it = tree.lookupExact(0);
    EXPECT_EQ(it, tree.end());
}

TEST_F(BTreeFixture, leaf_node_split) {
    const size_t key_count = PAGE_SIZE / (sizeof(size_t) * 2) * 2;
    BTree<size_t, size_t> tree(db->vmcache, context->getWorkerId());
    for (size_t i = 0; i < key_count; i++) {
        tree.insert(i, i);
    }
    for (size_t i = 0; i < key_count; i++) {
        auto it = tree.lookup(i);
        EXPECT_NE(it, tree.end());
        ASSERT_EQ((*it).second, i);
    }
}

TEST_F(BTreeFixture, inner_node_split) {
    const size_t node_size = 128;
    const size_t key_count = node_size / (sizeof(size_t) * 2) * 512;
    BTree<size_t, size_t, node_size> tree(db->vmcache, context->getWorkerId());
    for (size_t i = 0; i < key_count; i++) {
        tree.insert(i, i);
    }
    for (size_t i = 0; i < key_count; i++) {
        auto it = tree.lookup(i);
        EXPECT_NE(it, tree.end());
        ASSERT_EQ((*it).second, i);
    }
}

TEST_F(BTreeFixture, insertion_order) {
    const size_t key_count = PAGE_SIZE / (sizeof(size_t) * 2);
    BTree<size_t, size_t> tree(db->vmcache, context->getWorkerId());
    for (size_t i = key_count; i != 0; i--) {
        tree.insert(i, i);
    }
    for (size_t i = 1; i <= key_count; i++) {
        auto it = tree.lookup(i);
        EXPECT_NE(it, tree.end());
        ASSERT_EQ((*it).second, i);
    }
}

TEST_F(BTreeFixture, stress_100k_keys) {
    const size_t key_count = 100000;
    BTree<uint32_t, size_t> tree(db->vmcache, context->getWorkerId());
    size_t row = 0;
    for (size_t i = 0; i < key_count / 100 / 2; i++) {
        for (size_t key = i * 100; key < (i + 1) * 100; key++)
            tree.insert(key, row++);
        for (size_t key = key_count / 2 + i * 100; key < key_count / 2 + (i + 1) * 100; key++)
            tree.insert(key, row++);
    }

    for (size_t i = 0; i < key_count; i++) {
        auto it = tree.lookup(i);
        EXPECT_NE(it, tree.end());
    }
}

TEST_F(BTreeFixture, iterator) {
    BTree<Identifier, size_t> tree(db->vmcache, context->getWorkerId());
    // empty tree
    size_t num_values = 0;
    for (__attribute__((unused)) auto val : tree) {
        num_values++;
    }
    ASSERT_EQ(num_values, 0);

    // non-empty tree
    const size_t num_inserted_values = 100000;
    for (size_t i = 0; i < num_inserted_values; i++) {
        tree.insert(i, i);
    }
    for (auto val : tree) {
        EXPECT_EQ(val.first, num_values);
        EXPECT_EQ(val.second, num_values);
        num_values++;
    }
    EXPECT_EQ(num_values, num_inserted_values);
}

TEST_F(BTreeFixture, range_lookup) {
    const Identifier distinct_values_per_dim = 10;
    BTree<CompositeKey<4>, size_t> tree(db->vmcache, context->getWorkerId());
    for (Identifier i = 0; i < distinct_values_per_dim; i++) {
        for (Identifier j = 0; j < distinct_values_per_dim; j++) {
            for (Identifier k = 0; k < distinct_values_per_dim; k++) {
                for (Identifier l = 0; l < distinct_values_per_dim; l++) {
                    tree.insert(CompositeKey<4> { i, j, k, l }, 0);
                }
            }
        }
    }

    for (Identifier i = 0; i < distinct_values_per_dim; i++) {
        for (Identifier j = 0; j < distinct_values_per_dim; j++) {
            for (Identifier k = 0; k < distinct_values_per_dim; k++) {
                CompositeKey<4> from_key { i, j, k, 0 };
                auto it = tree.lookup(from_key);
                size_t num_values = 0;
                while (it != tree.end()) {
                    auto val = *it;
                    if (memcmp(&val.first, &from_key, sizeof(Identifier) * 3) != 0)
                        break;
                    EXPECT_EQ(val.first.keys[3], num_values);
                    EXPECT_EQ(val.second, 0);
                    num_values++;
                    ++it;
                }
                ASSERT_EQ(num_values, distinct_values_per_dim);
            }
        }
    }
}