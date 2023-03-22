#pragma once

#include "../../storage/page.hpp"
#include "../../storage/vmcache.hpp"

/**
 * B+-Tree implementation on top of 'VMCache' pages; requires key values to be unique
 */
template <typename KeyType, size_t capacity>
struct BTreeInnerNode {
    size_t n_keys;
    size_t level;
    PageId children[capacity + 1];
    KeyType keys[capacity];
};

template <typename KeyType, typename ValueType, size_t capacity>
struct BTreeLeafNode {
    size_t n_keys;
    PageId next;
    KeyType keys[capacity];
    ValueType values[capacity];
};

template <typename KeyType>
size_t lowerBound(const KeyType array[], size_t size, KeyType key) {
    if (size == 0)
        return 0;

    size_t l = 0;
    size_t h = size;
    while (l < h) {
        size_t m = (l + h) / 2;
        if (array[m] == key) {
            return m;
        } else if (array[m] > key) {
            h = m;
        } else {
            l = m + 1;
        }
    }
    return l;
}

template <typename KeyType, typename ValueType, size_t node_size = PAGE_SIZE>
class BTree {
    static const size_t inner_capacity = (node_size - sizeof(size_t) - sizeof(size_t) - sizeof(PageId)) / (sizeof(KeyType) + sizeof(PageId));
    typedef BTreeInnerNode<KeyType, inner_capacity> InnerNode;
    static const size_t leaf_capacity = (node_size - sizeof(size_t) - sizeof(PageId)) / (sizeof(KeyType) + sizeof(ValueType));
    typedef BTreeLeafNode<KeyType, ValueType, leaf_capacity> LeafNode;

    static_assert(sizeof(InnerNode) <= node_size);
    static_assert(sizeof(LeafNode) <= node_size);

public:
    struct Iterator {
        using iterator_category = std::input_iterator_tag;
        using difference_type = std::ptrdiff_t;
        using value_type = std::pair<KeyType, ValueType>;
        using pointer = value_type*;

        Iterator(VMCache& vmcache, PageId pid, LeafNode* page, size_t i, const uint32_t worker_id)
        : vmcache(vmcache)
        , pid(pid)
        , page(page)
        , i(i)
        , worker_id(worker_id) { }

        Iterator(VMCache& vmcache, const uint32_t worker_id)
        : vmcache(vmcache)
        , pid(INVALID_PAGE_ID)
        , page(nullptr)
        , i(0)
        , worker_id(worker_id) { }

        ~Iterator() {
            if (page != nullptr)
                vmcache.unfixShared(pid);
        }

        Iterator(const Iterator& other) // copy
        : vmcache(other.vmcache)
        , pid(other.pid)
        , page(other.page)
        , i(other.i)
        , worker_id(other.worker_id) {
            if (page != nullptr)
                vmcache.fixShared(pid, worker_id);
        }

        Iterator& operator=(const Iterator& other) { // copy
            if (this == &other)
                return *this;
            // clean up this
            if (page != nullptr)
                vmcache.unfixShared(pid);
            // copy
            vmcache = other.vmcache;
            pid = other.pid;
            page = other.page;
            i = other.i;
            worker_id = other.worker_id;
            if (page != nullptr)
                vmcache.fixShared(pid, worker_id);
            return *this;
        }

        Iterator(Iterator&& other) noexcept // move
        : vmcache(other.vmcache)
        , pid(other.pid)
        , page(other.page)
        , i(other.i)
        , worker_id(other.worker_id) {
            // shared latch on 'pid' potentially held by 'other' is now owned by 'this'
            other.pid = INVALID_PAGE_ID;
            other.page = nullptr;
            other.i = 0;
        }

        Iterator& operator=(Iterator&& other) noexcept { // move
            // clean up this
            if (page != nullptr)
                vmcache.unfixShared(pid);
            // move
            assert(&vmcache == &other.vmcache);
            pid = other.pid;
            page = other.page;
            i = other.i;
            worker_id = other.worker_id;
            // shared latch on 'pid' potentially held by 'other' is now owned by 'this'
            other.pid = INVALID_PAGE_ID;
            other.page = nullptr;
            other.i = 0;
            return *this;
        }

        value_type operator*() const {
            return std::make_pair(page->keys[i], page->values[i]);
        }

        Iterator& operator++() {
            if (page != nullptr && i + 1 >= page->n_keys) {
                i = 0;
                PageId next_pid = page->next;
                vmcache.unfixShared(pid);
                pid = next_pid;
                if (pid != INVALID_PAGE_ID) {
                    page = reinterpret_cast<LeafNode*>(vmcache.fixShared(pid, worker_id));
                } else {
                    page = nullptr;
                }
            } else {
                i++;
            }
            return *this;
        }

        friend bool operator== (const Iterator& a, const Iterator& b) { return a.pid == b.pid && a.i == b.i; };
        friend bool operator!= (const Iterator& a, const Iterator& b) { return a.pid != b.pid || a.i != b.i; };

    private:
        VMCache& vmcache;
        PageId pid;
        LeafNode* page;
        size_t i;
        uint32_t worker_id;
    };

    BTree(VMCache& vmcache, PageId root_pid, const uint32_t worker_id) : vmcache(vmcache), root_pid(root_pid), worker_id(worker_id) { }
    BTree(VMCache& vmcache, const uint32_t worker_id) : vmcache(vmcache), worker_id(worker_id) {
        root_pid = vmcache.allocatePage();
        InnerNode* root = reinterpret_cast<InnerNode*>(vmcache.fixExclusive(root_pid, worker_id));
        root->n_keys = 0;
        root->level = 1;
        root->children[0] = vmcache.allocatePage();
        LeafNode* leaf = reinterpret_cast<LeafNode*>(vmcache.fixExclusive(root->children[0], worker_id));
        leaf->n_keys = 0;
        leaf->next = INVALID_PAGE_ID;
        vmcache.unfixExclusive(root->children[0]);
        vmcache.unfixExclusive(root_pid);
    }

    Iterator begin() const {
        // get first leaf node
        PageId current_pid = root_pid;
        InnerNode* current = reinterpret_cast<InnerNode*>(vmcache.fixShared(current_pid, worker_id));
        PageId leaf_pid;
        LeafNode* leaf = nullptr;
        while (leaf == nullptr) {
            auto new_current_pid = current->children[0];
            auto current_level = current->level;
            vmcache.unfixShared(current_pid);
            if (current_level == 1) {
                leaf_pid = new_current_pid;
                leaf = reinterpret_cast<LeafNode*>(vmcache.fixShared(new_current_pid, worker_id));
            } else {
                current = reinterpret_cast<InnerNode*>(vmcache.fixShared(new_current_pid, worker_id));
                current_pid = new_current_pid;
            }
        }
        if (leaf->n_keys == 0) {
            vmcache.unfixShared(leaf_pid);
            return end();
        } else {
            return Iterator(vmcache, leaf_pid, leaf, 0, worker_id);
        }
    }

    Iterator end() const {
        return Iterator(vmcache, worker_id);
    }

    void insert(KeyType key, ValueType value) {
        PageId current_pid = root_pid;
        InnerNode* current = reinterpret_cast<InnerNode*>(vmcache.fixShared(current_pid, worker_id));
        const size_t max_stack_size = 16;
        PageId stack[max_stack_size];
        uint8_t stack_size = 0;
        PageId leaf_pid;
        LeafNode* leaf = nullptr;
        // find the correct leaf node
        while (leaf == nullptr) {
            size_t l = lowerBound<KeyType>(current->keys, current->n_keys, key);
            if (l < current->n_keys && current->keys[l] == key)
                l++;
            assert(l <= current->n_keys);
            auto new_current_pid = current->children[l];
            auto current_level = current->level;
            vmcache.unfixShared(current_pid);
            assert(stack_size < max_stack_size);
            stack[stack_size++] = current_pid;
            if (current_level == 1) {
                leaf_pid = new_current_pid;
                leaf = reinterpret_cast<LeafNode*>(vmcache.fixExclusive(new_current_pid, worker_id));
            } else {
                current = reinterpret_cast<InnerNode*>(vmcache.fixShared(new_current_pid, worker_id));
                current_pid = new_current_pid;
            }
        }
        insertIntoLeaf(leaf, leaf_pid, stack, stack_size, key, value);
    }

    Iterator lookup(KeyType key) {
        PageId current_pid = root_pid;
        InnerNode* current = reinterpret_cast<InnerNode*>(vmcache.fixShared(current_pid, worker_id));
        PageId leaf_pid;
        LeafNode* leaf = nullptr;
        // find the correct leaf node
        while (leaf == nullptr) {
            size_t l = lowerBound<KeyType>(current->keys, current->n_keys, key);
            if (l < current->n_keys && current->keys[l] == key)
                l++;
            assert(l <= current->n_keys);
            auto new_current_pid = current->children[l];
            auto current_level = current->level;
            vmcache.unfixShared(current_pid);
            if (current_level == 1) {
                leaf_pid = new_current_pid;
                leaf = reinterpret_cast<LeafNode*>(vmcache.fixShared(new_current_pid, worker_id));
            } else {
                current = reinterpret_cast<InnerNode*>(vmcache.fixShared(new_current_pid, worker_id));
                current_pid = new_current_pid;
            }
        }
        // search the key within the leaf node
        size_t l = lowerBound<KeyType>(leaf->keys, leaf->n_keys, key);
        return Iterator(vmcache, leaf_pid, leaf, l, worker_id);
    }

    Iterator lookupExact(KeyType key) {
        auto it = lookup(key);
        if ((*it).first == key) {
            return it;
        } else {
            return Iterator(vmcache, worker_id); // invalid iterator
        }
    }

    PageId getRootPid() const {
        return root_pid;
    }

private:
    struct LeafNodeInfo {
        LeafNode* leaf;
        PageId pid;
    };

    struct InnerNodeInfo {
        InnerNode* inner;
        PageId pid;
    };

    void insertIntoLeaf(LeafNode* leaf, PageId leaf_pid, PageId* stack, size_t stack_size, KeyType key, ValueType value) {
        // search the key within the leaf node
        size_t l = lowerBound<KeyType>(leaf->keys, leaf->n_keys, key);
        if (l < leaf->n_keys && leaf->keys[l] == key) {
            vmcache.unfixExclusive(leaf_pid);
            throw std::runtime_error("Key already exists!");
        } else {
            // insert new key
            if (leaf->n_keys == leaf_capacity) {
                // split leaf node
                LeafNodeInfo new_leaf = splitLeaf(leaf, stack, stack_size);
                // setup for actual key insertion
                if (l > leaf->n_keys) {
                    // key goes into the new leaf node
                    l -= leaf->n_keys;
                    std::swap(new_leaf.leaf, leaf);
                    std::swap(new_leaf.pid, leaf_pid);
                }
                vmcache.unfixExclusive(new_leaf.pid);
            }

            for (size_t i = leaf->n_keys; i > l; i--) {
                leaf->keys[i] = leaf->keys[i - 1];
                leaf->values[i] = leaf->values[i - 1];
            }
            leaf->keys[l] = key;
            leaf->values[l] = value;
            leaf->n_keys++;
            vmcache.unfixExclusive(leaf_pid);
        }
    }

    LeafNodeInfo splitLeaf(LeafNode* leaf, PageId* stack, size_t stack_size) {
        assert(stack_size > 0);
        PageId new_leaf_pid = vmcache.allocatePage();
        LeafNode* new_leaf = reinterpret_cast<LeafNode*>(vmcache.fixExclusive(new_leaf_pid, worker_id));
        new_leaf->n_keys = leaf_capacity / 2;
        new_leaf->next = leaf->next;
        leaf->n_keys = (leaf_capacity + 1) / 2;
        leaf->next = new_leaf_pid;
        memcpy(new_leaf->keys, leaf->keys + leaf->n_keys, new_leaf->n_keys * sizeof(KeyType));
        memcpy(new_leaf->values, leaf->values + leaf->n_keys, new_leaf->n_keys * sizeof(ValueType));
        PageId parent = stack[--stack_size];
        insertIntoInner(parent, stack, stack_size, new_leaf->keys[0], new_leaf_pid);
        return LeafNodeInfo { new_leaf, new_leaf_pid };
    }

    void insertIntoInner(PageId inner_pid, PageId* stack, size_t stack_size, KeyType key, PageId child) {
        InnerNode* inner = reinterpret_cast<InnerNode*>(vmcache.fixExclusive(inner_pid, worker_id));
        size_t l = lowerBound(inner->keys, inner->n_keys, key);
        if (inner->n_keys == inner_capacity) {
            InnerNodeInfo new_inner = splitInner(inner, inner_pid, stack, stack_size);
            // setup for actual key insertion
            if (l > inner->n_keys) {
                // key goes into the new inner node
                l -= inner->n_keys + 1;
                std::swap(new_inner.inner, inner);
                std::swap(new_inner.pid, inner_pid);
            }
            vmcache.unfixExclusive(new_inner.pid);
        }
        // insert 'key' into inner node
        for (size_t i = inner->n_keys; i > l; i--) {
            inner->keys[i] = inner->keys[i - 1];
        }
        for (size_t i = inner->n_keys + 1; i > l + 1; i--) {
            inner->children[i] = inner->children[i - 1];
        }
        inner->keys[l] = key;
        inner->children[l + 1] = child;
        inner->n_keys++;
        vmcache.unfixExclusive(inner_pid);
    }

    InnerNodeInfo splitInner(InnerNode*& inner, PageId& inner_pid, PageId* stack, size_t stack_size) {
        PageId new_inner_pid = vmcache.allocatePage();
        InnerNode* new_inner = reinterpret_cast<InnerNode*>(vmcache.fixExclusive(new_inner_pid, worker_id));
        new_inner->n_keys = inner_capacity / 2 - 1;
        new_inner->level = inner->level;
        inner->n_keys = (inner_capacity + 1) / 2;
        assert(inner->n_keys + new_inner->n_keys == inner_capacity - 1);
        memcpy(new_inner->keys, inner->keys + inner->n_keys + 1, new_inner->n_keys * sizeof(KeyType));
        memcpy(new_inner->children, inner->children + inner->n_keys + 1, (new_inner->n_keys + 1) * sizeof(PageId));
        const KeyType split_key = inner->keys[inner->n_keys];
        if (stack_size == 0) {
            // we just split the root page, so we need to create a new 'inner', transfer its contents to 'inner' and update it to point only to 'inner' and 'new_inner'
            PageId root_pid = inner_pid;
            InnerNode* root = inner;
            // copy 'root' to new 'inner'
            inner_pid = vmcache.allocatePage();
            inner = reinterpret_cast<InnerNode*>(vmcache.fixExclusive(inner_pid, worker_id));
            inner->level = root->level;
            inner->n_keys = root->n_keys;
            memcpy(inner->keys, root->keys, inner->n_keys * sizeof(KeyType));
            memcpy(inner->children, root->children, (inner->n_keys + 1) * sizeof(PageId));
            // recreate 'root'
            root->n_keys = 1;
            root->level = 0; // TODO: does it make sense to use 'new_inner->level + 1' here, such that 'level' monotonically decreases from the root down?
            root->keys[0] = split_key;
            root->children[0] = inner_pid;
            root->children[1] = new_inner_pid;
            vmcache.unfixExclusive(root_pid);
        } else {
            PageId parent = stack[--stack_size];
            insertIntoInner(parent, stack, stack_size, split_key, new_inner_pid);
        }
        return InnerNodeInfo { new_inner, new_inner_pid };
    }

    VMCache& vmcache;
    PageId root_pid;
    const uint32_t worker_id;
};