#include "test/shared/db_test.hpp"
#include "prototype/core/db.hpp"
#include "prototype/core/types.hpp"
#include "prototype/scheduling/execution_context.hpp"
#include "prototype/storage/persistence/btree.hpp"
#include "prototype/storage/persistence/table.hpp"

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

class BTreeFixture : public DBTestFixture {
protected:
    template <typename KeyType, typename ValueType>
    PageId getRootPid(BTree<KeyType, ValueType> tree) { return tree.root_pid; }
};

TEST_F(BTreeFixture, double_insert) {
    BTree<uint32_t, size_t> tree(db->vmcache, context->getWorkerId());
    tree.insert(1, 1);
    EXPECT_ANY_THROW(tree.insert(1, 2));
}

TEST_F(BTreeFixture, remove) {
    BTree<uint32_t, size_t> tree(db->vmcache, context->getWorkerId());
    tree.insert(1, 4);
    tree.insert(2, 3);
    tree.insert(3, 2);
    tree.insert(4, 1);
    auto it = tree.lookupExact(2);
    EXPECT_NE(it, tree.end());
    EXPECT_EQ((*it).first, 2);
    EXPECT_EQ((*it).second, 3);
    EXPECT_EQ(tree.getCardinality(), 4);
    it.release();
    tree.remove(2);
    it = tree.lookupExact(2);
    EXPECT_TRUE(it == tree.end());
    EXPECT_EQ(tree.getCardinality(), 3);
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
    // exact lookups of non-existent keys should return the end() iterator
    it = tree.lookupExact(0);
    EXPECT_EQ(it, tree.end());
    // looking up a key beyond the key range present in the tree should return the end() iterator
    it = tree.lookup(413);
    EXPECT_EQ(it, tree.end());

    // test lookupValue()
    EXPECT_EQ(tree.lookupValue(413), std::nullopt);
    EXPECT_EQ(tree.lookupValue(1).value(), 4);
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

TEST_F(BTreeFixture, cardinality) {
    const size_t key_count = 918;
    BTree<size_t, size_t> tree(db->vmcache, context->getWorkerId());
    for (size_t i = 0; i < key_count; i++) {
        tree.insert(i, i);
    }
    ASSERT_EQ(tree.getCardinality(), key_count);
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

const size_t NODE_SIZE = 128;
using TestBTree = BTree<size_t, size_t, NODE_SIZE>;
void checkLevel(PageId pid, size_t expected_level, const std::shared_ptr<DB>& db, const std::shared_ptr<ExecutionContext>& context) {
    TestBTree::InnerNode* current = reinterpret_cast<TestBTree::InnerNode*>(db->vmcache.fixShared(pid, context->getWorkerId()));
    for (size_t i = 0; i < current->n_keys + 1; i++) {
        ASSERT_EQ(current->level, expected_level);
        if (current->level != 1)
            checkLevel(current->children[i], current->level - 1, db, context);
    }
    db->vmcache.unfixShared(pid);
}

// ensure that the 'level' of BTree nodes decreases monotonically from the root
TEST_F(BTreeFixture, monotonic_levels) {
    const size_t key_count = NODE_SIZE / (sizeof(size_t) * 2) * 512;
    TestBTree tree(db->vmcache, context->getWorkerId());
    for (size_t i = 0; i < key_count; i++) {
        tree.insert(i, i);
    }

    PageId current_pid = tree.getRootPid();
    TestBTree::InnerNode* current = reinterpret_cast<TestBTree::InnerNode*>(db->vmcache.fixShared(current_pid, context->getWorkerId()));
    for (size_t i = 0; i < current->n_keys + 1; i++) {
        ASSERT_GT(current->level, 1);
        checkLevel(current->children[i], current->level - 1, db, context);
    }
    db->vmcache.unfixShared(current_pid);
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
    size_t max_key = 0;
    for (size_t i = 0; i < key_count / 100 / 2; i++) {
        for (size_t key = i * 100; key < (i + 1) * 100; key++) {
            tree.insert(key, row++);
            max_key = std::max(key, max_key);
        }
        for (size_t key = key_count / 2 + i * 100; key < key_count / 2 + (i + 1) * 100; key++) {
            tree.insert(key, row++);
            max_key = std::max(key, max_key);
        }
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

    // descending scan
    num_values = 0;
    for (auto it = --tree.end(); ; --it) {
        num_values++;
        auto val = *it;
        EXPECT_EQ(val.first, num_inserted_values - num_values);
        EXPECT_EQ(val.second, num_inserted_values - num_values);
        if (it == tree.begin())
            break;
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

TEST_F(BTreeFixture, bool_values) {
    BTree<Identifier, bool> tree(db->vmcache, context->getWorkerId());
    std::vector<std::pair<Identifier, bool>> test_values = {
        { 5, true },
        { 3, true },
        { 4, false },
        { 7, false },
        { 9, true },
        { 2, false },
        { 15, true },
        { 14, false },
        { 13, false },
        { 12, true },
        { 11, false },
        { 10, false },
        { 8, true },
        { 6, false },
        { 1, false },
        { 0, true },
        { 18, false },
        { 21, true }
    };
    for (auto p : test_values) {
        tree.insert(p.first, p.second);
    }
    for (auto p : test_values) {
        auto it = tree.lookupExact(p.first);
        EXPECT_NE(it, tree.end());
        EXPECT_EQ((*it).second, p.second);
    }
}

TEST_F(BTreeFixture, bool_values_leaf_node_split) {
    const size_t key_count = (PAGE_SIZE * 8 / (sizeof(Identifier) * 8 + 1)) * 2;
    BTree<Identifier, bool> tree(db->vmcache, context->getWorkerId());
    for (size_t i = 0; i < key_count; i++) {
        tree.insert(i, static_cast<bool>(i % 7));
    }
    for (size_t i = 0; i < key_count; i++) {
        auto it = tree.lookupExact(i);
        EXPECT_NE(it, tree.end());
        ASSERT_EQ((*it).second, static_cast<bool>(i % 7));
    }
}

TEST_F(BTreeFixture, insertNext) {
    const size_t key_count = (PAGE_SIZE * 8 / (sizeof(Identifier) * 8 + 1)) * 2;
    BTree<RowId, bool> tree(db->vmcache, context->getWorkerId());
    for (size_t i = 0; i < key_count; i++) {
        ASSERT_EQ(tree.insertNext(static_cast<bool>(i % 7)).key, i);
    }
    for (size_t i = 0; i < key_count; i++) {
        auto it = tree.lookupExact(i);
        EXPECT_NE(it, tree.end());
        ASSERT_EQ((*it).second, static_cast<bool>(i % 7));
    }
}

TEST_F(BTreeFixture, latchForUpdate) {
    const size_t key_count = (PAGE_SIZE * 8 / (sizeof(Identifier) * 8 + 1)) * 2;
    BTree<RowId, bool> tree(db->vmcache, context->getWorkerId());
    for (size_t i = 0; i < key_count; i++) {
        tree.insertNext(static_cast<bool>(i % 7));
    }
    for (size_t i = 0; i < key_count; i++) {
        auto guard = tree.latchForUpdate(i);
        EXPECT_TRUE(guard.has_value());
        EXPECT_EQ(guard->prev_value, static_cast<bool>(i % 7));
    }

    EXPECT_FALSE(tree.latchForUpdate(key_count).has_value());
}

TEST_F(BTreeFixture, keyRange) {
    BTree<RowId, bool> tree(db->vmcache, context->getWorkerId());
    auto range = tree.keyRange();
    ASSERT_EQ(range.first, 0);
    ASSERT_EQ(range.second, 0);

    ASSERT_EQ(tree.insertNext(true).key, 0);
    range = tree.keyRange();
    ASSERT_EQ(range.first, 0);
    ASSERT_EQ(range.second, 1);

    for (size_t i = 0; i < 3; i++) {
        tree.insertNext(true);
    }
    range = tree.keyRange();
    ASSERT_EQ(range.first, 0);
    ASSERT_EQ(range.second, 4);
}

TEST_F(BTreeFixture, merge) {
    BTree<RowId, uint64_t> tree(db->vmcache, context->getWorkerId());
    typedef BTree<RowId, uint64_t>::InnerNode InnerNode;
    typedef BTree<RowId, uint64_t>::LeafNode LeafNode;

    // setup tree with a root and two leaves
    ASSERT_EQ(SharedGuard<InnerNode>(db->vmcache, getRootPid(tree), context->getWorkerId())->level, 1);
    ASSERT_EQ(SharedGuard<InnerNode>(db->vmcache, getRootPid(tree), context->getWorkerId())->n_keys, 0);
    for (size_t i = 0; i < InnerNode::capacity + 1; i++) {
        ASSERT_EQ(tree.insertNext(i).key, i);
    }
    ASSERT_EQ(SharedGuard<InnerNode>(db->vmcache, getRootPid(tree), context->getWorkerId())->level, 1);
    ASSERT_EQ(SharedGuard<InnerNode>(db->vmcache, getRootPid(tree), context->getWorkerId())->n_keys, 1);

    // provoke merge by removing entries from left leaf
    const size_t pre_merge_removals = InnerNode::capacity / 4;
    for (size_t i = 0; i < pre_merge_removals; i++) {
        ASSERT_EQ(tree.remove(i), true);
        ASSERT_EQ(SharedGuard<InnerNode>(db->vmcache, getRootPid(tree), context->getWorkerId())->level, 1);
        ASSERT_EQ(SharedGuard<InnerNode>(db->vmcache, getRootPid(tree), context->getWorkerId())->n_keys, 1);
    }
    // this remove() should cause a merge
    ASSERT_EQ(tree.remove(pre_merge_removals), true);
    ASSERT_EQ(SharedGuard<InnerNode>(db->vmcache, getRootPid(tree), context->getWorkerId())->n_keys, 0);

    // make sure that we can still access all keys remaining in the tree after the merge
    ASSERT_EQ(tree.getCardinality(), InnerNode::capacity - pre_merge_removals);
    for (size_t i = pre_merge_removals + 1; i < InnerNode::capacity + 1; i++) {
        auto it = tree.lookupExact(i);
        ASSERT_NE(it, tree.end());
        ASSERT_EQ((*it).second, i);
    }
}

TEST(BTree, InnerNode_remove) {
    BTreeInnerNode<RowId, PAGE_SIZE> node;
    node.n_keys = 4;
    node.keys[0] = 0;
    node.keys[1] = 1;
    node.keys[2] = 2;
    node.keys[3] = 3;
    node.children[0] = 1;
    node.children[1] = 2;
    node.children[2] = 3;
    node.children[3] = 4;
    node.children[4] = 5;

    node.remove(1); // middle child
    EXPECT_EQ(node.n_keys, 3);
    EXPECT_EQ(node.keys[0], 1);
    EXPECT_EQ(node.keys[1], 2);
    EXPECT_EQ(node.keys[2], 3);
    EXPECT_EQ(node.children[0], 1);
    EXPECT_EQ(node.children[1], 3);
    EXPECT_EQ(node.children[2], 4);
    EXPECT_EQ(node.children[3], 5);

    node.remove(3); // right child
    EXPECT_EQ(node.n_keys, 2);
    EXPECT_EQ(node.keys[0], 1);
    EXPECT_EQ(node.keys[1], 2);
    EXPECT_EQ(node.children[0], 1);
    EXPECT_EQ(node.children[1], 3);
    EXPECT_EQ(node.children[2], 4);

    node.remove(0); // left child
    EXPECT_EQ(node.n_keys, 1);
    EXPECT_EQ(node.keys[0], 1);
    EXPECT_EQ(node.children[0], 3);
    EXPECT_EQ(node.children[1], 4);
}