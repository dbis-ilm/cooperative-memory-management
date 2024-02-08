#pragma once

#include <optional>

#include "../../storage/guard.hpp"
#include "../../storage/page.hpp"
#include "../../storage/vmcache.hpp"

/**
 * B+-Tree implementation on top of 'VMCache' pages; requires key values to be unique
 */
template <typename KeyType>
struct BTreeNodeHeader {
    size_t n_keys;
    size_t level;
    KeyType lfence;
    KeyType rfence;
};

template <typename KeyType, size_t size>
struct BTreeInnerNode : BTreeNodeHeader<KeyType> {
    static_assert(sizeof(BTreeNodeHeader<KeyType>) - sizeof(PageId) < size);
    static const size_t capacity = (size - sizeof(BTreeNodeHeader<KeyType>) - sizeof(PageId)) / (sizeof(KeyType) + sizeof(PageId));
    static_assert(capacity >= 1);

    PageId children[capacity + 1];
    KeyType keys[capacity];

    inline void remove(size_t i) {
        // TODO: FIXME for final leaf merge implementation
        assert(i <= this->n_keys);
        if (i != this->n_keys)
            memmove(this->keys + i, this->keys + i + 1, sizeof(KeyType) * (this->n_keys - i - 1));
        memmove(this->children + i, this->children + i + 1, sizeof(PageId) * (this->n_keys - i));
        this->n_keys--;
    }
};

template <typename KeyType, typename ValueType, size_t size>
struct BTreeLeafNode : BTreeNodeHeader<KeyType> {
    static_assert(sizeof(BTreeNodeHeader<KeyType>) - sizeof(PageId) < size);
    static const size_t capacity = (size - sizeof(BTreeNodeHeader<KeyType>) - sizeof(PageId)) / (sizeof(KeyType) + sizeof(ValueType));
    static_assert(capacity >= 1);

    PageId next;
    KeyType keys[capacity];
    ValueType values[capacity];

    inline KeyType split(ExclusiveGuard<BTreeLeafNode>& new_leaf) {
        size_t l_n_keys = (this->n_keys + 1) / 2;
        // after split: this = left node, new_leaf = right node
        new_leaf->n_keys = this->n_keys - l_n_keys;
        this->n_keys = l_n_keys;
        memcpy(new_leaf->keys, this->keys + l_n_keys, new_leaf->n_keys * sizeof(KeyType));
        memcpy(new_leaf->values, this->values + l_n_keys, new_leaf->n_keys * sizeof(ValueType));
        return new_leaf->keys[0];
    }

    inline void insert(size_t i, KeyType key, ValueType value) {
        for (size_t j = this->n_keys; j > i; j--) {
            this->keys[j] = this->keys[j - 1];
            this->values[j] = this->values[j - 1];
        }
        this->keys[i] = key;
        this->values[i] = value;
        this->n_keys++;
    }

    inline ValueType get(size_t i) const {
        return this->values[i];
    }

    inline void remove(size_t i) {
        assert(i < this->n_keys);
        memmove(this->keys + i, this->keys + i + 1, sizeof(KeyType) * (this->n_keys - i - 1));
        memmove(this->values + i, this->values + i + 1, sizeof(ValueType) * (this->n_keys - i - 1));
        this->n_keys--;
    }

    // merge 'right' into this
    inline bool merge(size_t i, BTreeInnerNode<KeyType, size>* parent, BTreeLeafNode<KeyType, ValueType, size>* right) {
        if (this->n_keys + right->n_keys > capacity)
            return false;

        // merge leaf data
        memcpy(this->keys + this->n_keys, right->keys, right->n_keys * sizeof(KeyType));
        memcpy(this->values + this->n_keys, right->values, right->n_keys * sizeof(ValueType));
        this->n_keys += right->n_keys;
        this->next = right->next;
        this->rfence = right->rfence;
        // update parent node
        parent->remove(i + 1);
        return true;
    }
};

template <typename KeyType, size_t size>
struct BTreeLeafNode<KeyType, bool, size> : BTreeNodeHeader<KeyType> {
    static_assert(sizeof(BTreeNodeHeader<KeyType>) - sizeof(PageId) < size);
    static const size_t capacity = (size - sizeof(BTreeNodeHeader<KeyType>) - sizeof(PageId)) * 8 / (sizeof(KeyType) * 8 + 1);
    static_assert(capacity >= 1);

    PageId next;
    KeyType keys[capacity];
    uint8_t values[(capacity + 7) / 8];

    inline KeyType split(ExclusiveGuard<BTreeLeafNode>& new_leaf) {
        size_t l_n_keys = (this->n_keys + 7) / 16 * 8; // split at a multiple of 8 to simplify copying values
        // after split: this = left node, new_leaf = right node
        new_leaf->n_keys = this->n_keys - l_n_keys;
        this->n_keys = l_n_keys;
        memcpy(new_leaf->keys, this->keys + l_n_keys, new_leaf->n_keys * sizeof(KeyType));
        memcpy(new_leaf->values, this->values + l_n_keys / 8, (new_leaf->n_keys + 7) / 8);
        return new_leaf->keys[0];
    }

    inline void insert(size_t i, KeyType key, bool value) {
        for (size_t j = this->n_keys; j > i; j--) {
            this->keys[j] = this->keys[j - 1];
        }
        // TODO: shifting existing values back could probably be made more efficient here, but the expected use case for this type of B+-Tree is append-only workloads, so it should not matter anyways
        size_t byte_pos = i / 8;
        size_t bit_pos = i % 8;
        for (size_t j = (this->n_keys + 7) / 8; j > byte_pos; j--) {
            this->values[j] <<= 1;
            if (this->values[j - 1] & 0x80)
                this->values[j] |= 0x1;
            else
                this->values[j] &= ~0x1;
        }
        this->keys[i] = key;
        uint8_t prev_val_mask = ~((0x1u << bit_pos) - 1u);
        uint8_t prev_values = this->values[byte_pos] & prev_val_mask;
        this->values[byte_pos] &= ~prev_val_mask;
        this->values[byte_pos] |= prev_values << 1;
        this->values[byte_pos] |= value << bit_pos;
        this->n_keys++;
    }

    inline bool get(size_t i) const {
        return (this->values[i / 8] >> (i % 8)) & 0x1;
    }

    inline void update(size_t i, bool value) {
        if (value) {
            this->values[i / 8] |= 0x1 << (i % 8);
        } else {
            this->values[i / 8] &= ~(0x1 << (i % 8));
        }
    }

    inline void remove(size_t) {
        throw std::runtime_error("Removal is not supported yet for bool-valued B+-Trees!");
    }

    inline bool merge(size_t, BTreeInnerNode<KeyType, size>*, BTreeLeafNode<KeyType, bool, size>*) {
        throw std::runtime_error("Leaf node merging is not supported yet for bool-valued B+-Trees!");
    }
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
public:
    typedef BTreeInnerNode<KeyType, node_size> InnerNode;
    typedef BTreeLeafNode<KeyType, ValueType, node_size> LeafNode;

    static_assert(sizeof(InnerNode) <= node_size);
    static_assert(sizeof(LeafNode) <= node_size);

    struct Iterator {
        using iterator_category = std::input_iterator_tag;
        using difference_type = std::ptrdiff_t;
        using value_type = std::pair<KeyType, ValueType>;
        using pointer = value_type*;

        Iterator(const BTree* tree, SharedGuard<LeafNode>&& page, size_t i, const uint32_t worker_id)
        : tree(tree)
        , page(std::move(page))
        , last_pid(0)
        , i(i)
        , worker_id(worker_id) { }

        Iterator(const BTree* tree, VMCache& vmcache, const uint32_t worker_id)
        : tree(tree)
        , page(vmcache, worker_id)
        , last_pid(0)
        , i(END_I)
        , worker_id(worker_id) { }

        value_type operator*() {
            ensurePageLoaded();
            return std::make_pair(page->keys[i], page->get(i));
        }

        Iterator& operator++() {
            if (page.isReleased() && i == END_I) // end(), do nothing
                return *this;
            ensurePageLoaded();
            i++;
            if (i >= page->n_keys) {
                i = 0;
                if (page->next != INVALID_PAGE_ID) {
                    page = SharedGuard<LeafNode>(page.vmcache, page->next, worker_id);
                } else {
                    i = END_I;
                    page.release();
                }
            }
            return *this;
        }

        Iterator& operator--() {
            if (page.isReleased() && i == END_I) {
                page = SharedGuard<LeafNode>(page.vmcache, tree->getLastLeaf(), worker_id);
                i = page->n_keys - 1;
            } else {
                ensurePageLoaded();
                if (i == 0) {
                    // find previous leaf
                    KeyType key = page->lfence - 1;
                    PageId prev_pid = page.pid;
                    for (size_t repeat_counter = 0; ; repeat_counter++) {
                        try {
                            PageId stack[tree->max_stack_size];
                            uint8_t stack_size = 0;
                            page = SharedGuard<LeafNode>(page.vmcache, tree->traverse(key, stack, stack_size), worker_id);
                            break;
                        } catch (const OLRestartException&) { }
                    }
                    i = page->n_keys - 1;
                    if (page.pid == prev_pid) {
                        page.release();
                        i = END_I;
                    }
                } else {
                    i--;
                }
            }
            return *this;
        }

        void release() {
            if (!page.isReleased()) {
                last_pid = page.pid;
                page.release();
            }
        }

        friend bool operator== (const Iterator& a, const Iterator& b) { return a.page.pid == b.page.pid && a.i == b.i; };
        friend bool operator!= (const Iterator& a, const Iterator& b) { return a.page.pid != b.page.pid || a.i != b.i; };

    private:
        void ensurePageLoaded() {
            if (page.isReleased()) {
                assert(last_pid != 0);
                page = SharedGuard<LeafNode>(page.vmcache, last_pid, worker_id);
            }
        }

        static constexpr size_t END_I = std::numeric_limits<size_t>::max();

        const BTree* tree;
        SharedGuard<LeafNode> page;
        size_t last_pid;
        size_t i;
        uint32_t worker_id;
    };

    struct InsertGuard : public ExclusiveGuard<LeafNode> {
        const KeyType key;

        InsertGuard(ExclusiveGuard<LeafNode>&& guard, KeyType key) : ExclusiveGuard<LeafNode>(std::forward<ExclusiveGuard<LeafNode>>(guard)), key(key) { }
    };

    struct UpdateGuard : public ExclusiveGuard<LeafNode> {
        const ValueType prev_value;
        size_t index;

        UpdateGuard(ExclusiveGuard<LeafNode>&& guard, ValueType prev_value, size_t index) : ExclusiveGuard<LeafNode>(std::forward<ExclusiveGuard<LeafNode>>(guard)), prev_value(prev_value), index(index) { }

        void update(ValueType new_value) {
            this->data->update(index, new_value);
        }
    };

    BTree(VMCache& vmcache, PageId root_pid, const uint32_t worker_id) : vmcache(vmcache), root_pid(root_pid), worker_id(worker_id) { }
    BTree(VMCache& vmcache, const uint32_t worker_id) : vmcache(vmcache), worker_id(worker_id) {
        AllocGuard<InnerNode> root(vmcache, worker_id);
        root_pid = root.pid;
        root->n_keys = 0;
        root->level = 1;
        root->lfence = std::numeric_limits<KeyType>::max();
        root->rfence = std::numeric_limits<KeyType>::min();
        AllocGuard<LeafNode> leaf(vmcache, worker_id);
        root->children[0] = leaf.pid;
        leaf->n_keys = 0;
        leaf->next = INVALID_PAGE_ID;
        leaf->level = 0;
        leaf->lfence = std::numeric_limits<KeyType>::max();
        leaf->rfence = std::numeric_limits<KeyType>::min();
    }

    Iterator begin() const {
        // get first leaf node
        SharedGuard<LeafNode> leaf(vmcache, getFirstLeaf(), worker_id);
        if (leaf->n_keys == 0) {
            return end();
        } else {
            return Iterator(this, std::move(leaf), 0, worker_id);
        }
    }

    Iterator end() const {
        return Iterator(this, vmcache, worker_id);
    }

    size_t getCardinality() const {
        SharedGuard<LeafNode> leaf(vmcache, getFirstLeaf(), worker_id);
        size_t cardinality = leaf->n_keys;
        while (leaf->next != INVALID_PAGE_ID) {
            leaf = SharedGuard<LeafNode>(vmcache, leaf->next, worker_id);
            cardinality += leaf->n_keys;
        }
        return cardinality;
    }

    std::pair<KeyType, KeyType> keyRange() const {
        SharedGuard<InnerNode> root(vmcache, root_pid, worker_id);
        return std::make_pair(root->lfence, root->rfence);
    }

    const size_t max_stack_size = 16;
    // TODO: stack should hold OptimisticGuards that are revalidated on entering this method?!
    PageId traverse(KeyType key, PageId* stack, uint8_t& stack_size) const {
        OptimisticGuard<InnerNode> current(vmcache, stack_size == 0 ? root_pid : stack[--stack_size], worker_id);
        if (key < current->lfence || key >= current->rfence) { // restart
            stack_size = 0;
            current = OptimisticGuard<InnerNode>(vmcache, root_pid, worker_id);
        }
        // find the correct leaf node
        while (true) {
            size_t l = lowerBound<KeyType>(current->keys, current->n_keys, key);
            if (l < current->n_keys && current->keys[l] == key)
                l++;
            assert(l <= current->n_keys);
            assert(stack_size < max_stack_size);
            stack[stack_size++] = current.pid;
            if (current->level == 1) {
                return current->children[l];
            } else {
                current = OptimisticGuard<InnerNode>(vmcache, current->children[l], worker_id);
            }
        }
    }

    void insert(KeyType key, ValueType value) {
        for (size_t repeat_counter = 0; ; repeat_counter++) {
            try {
                PageId stack[max_stack_size];
                uint8_t stack_size = 0;
                ExclusiveGuard<LeafNode> leaf(vmcache, traverse(key, stack, stack_size), worker_id);
                insertIntoLeaf(std::move(leaf), stack, stack_size, key, value);
                return;
            } catch (const OLRestartException&) { }
        }
    }

    std::optional<UpdateGuard> latchForUpdate(KeyType key) {
        for (size_t repeat_counter = 0; ; repeat_counter++) {
            try {
                PageId stack[max_stack_size];
                uint8_t stack_size = 0;
                ExclusiveGuard<LeafNode> leaf(vmcache, traverse(key, stack, stack_size), worker_id);
                size_t l = lowerBound<KeyType>(leaf->keys, leaf->n_keys, key);
                if (l < leaf->n_keys && leaf->keys[l] == key)
                    return std::optional<UpdateGuard>(std::in_place, std::move(leaf), leaf->get(l), l);
                else
                    return std::nullopt;
            } catch (const OLRestartException&) { }
        }
    }

    // performs an insert at the next possible key value
    // returns the inserted key and an exclusive guard for the leaf page that the key was inserted into (this is to be used for insert operation synchronization)
    InsertGuard insertNext(ValueType value) {
        for (size_t repeat_counter = 0; ; repeat_counter++) {
            try {
                PageId stack[max_stack_size];
                uint8_t stack_size = 0;
                KeyType key = std::numeric_limits<KeyType>::max();
                ExclusiveGuard<LeafNode> leaf(vmcache, traverse(key, stack, stack_size), worker_id);
                if (leaf->n_keys == 0) {
                    key = {};
                } else {
                    key = leaf->keys[leaf->n_keys - 1] + 1;
                }
                leaf = insertIntoLeaf(std::move(leaf), stack, stack_size, key, value);
                return InsertGuard(std::move(leaf), key);
            } catch (const OLRestartException&) { }
        }
    }

    bool remove(KeyType key) {
        for (size_t repeat_counter = 0; ; repeat_counter++) {
            try {
                OptimisticGuard<InnerNode> parent(vmcache, root_pid, worker_id);
                if (key < parent->lfence || key >= parent->rfence) { // key not in tree
                    return false;
                }
                size_t leaf_pos;
                PageId leaf_pid;
                while (true) {
                    size_t l = lowerBound<KeyType>(parent->keys, parent->n_keys, key);
                    if (l < parent->n_keys && parent->keys[l] == key)
                        l++;
                    assert(l <= parent->n_keys);
                    if (parent->level == 1) {
                        leaf_pos = l;
                        leaf_pid = parent->children[l];
                        break;
                    } else {
                        parent = OptimisticGuard<InnerNode>(vmcache, parent->children[l], worker_id);
                    }
                }

                OptimisticGuard<LeafNode> leaf(vmcache, leaf_pid, worker_id);
                // search the key within the leaf node
                size_t l = lowerBound<KeyType>(leaf->keys, leaf->n_keys, key);
                if (l >= leaf->n_keys || leaf->keys[l] != key)
                    return false;
                // TODO: finalize and test merge implementation
                if (false && leaf->n_keys - 1 <= LeafNode::capacity / 4 && parent->n_keys >= 2 && (leaf_pos + 1) <= parent->n_keys) {
                    // underfull, attempt merge with next node
                    ExclusiveGuard<InnerNode> parent_x(std::move(parent));
                    ExclusiveGuard<LeafNode> leaf_x(std::move(leaf));
                    ExclusiveGuard<LeafNode> right_x(vmcache, parent_x->children[leaf_pos + 1], worker_id);
                    leaf_x->remove(l);
                    if (right_x->n_keys <= LeafNode::capacity / 4) { // TODO: does this condition make sense? e.g., if 'leaf' is completely empty, maybe we should merge regardless of the state of 'right'
                        if (leaf_x->merge(leaf_pos, parent_x.data, right_x.data)) {
                            // TODO: actually free up the page taken up by the right node
                        }
                    }
                } else {
                    ExclusiveGuard<LeafNode> leaf_x(std::move(leaf));
                    parent.release();
                    leaf_x->remove(l);
                }
                return true;
            } catch(const OLRestartException&) {}
        }
    }

    Iterator lookup(KeyType key) {
        for (size_t repeat_counter = 0; ; repeat_counter++) {
            try {
                PageId stack[max_stack_size];
                uint8_t stack_size = 0;
                SharedGuard<LeafNode> leaf(vmcache, traverse(key, stack, stack_size), worker_id);
                if ((leaf->n_keys == 0 || key > leaf->keys[leaf->n_keys - 1]) && leaf->next == INVALID_PAGE_ID)
                    return end();
                // search the key within the leaf node
                size_t l = lowerBound<KeyType>(leaf->keys, leaf->n_keys, key);
                return Iterator(this, std::move(leaf), l, worker_id);
            } catch (const OLRestartException&) { }
        }
    }

    Iterator lookupExact(KeyType key) {
        auto it = lookup(key);
        if (it != end() && (*it).first == key) {
            return it;
        } else {
            return end();
        }
    }

    std::optional<ValueType> lookupValue(KeyType key) {
        for (size_t repeat_counter = 0; ; repeat_counter++) {
            try {
                PageId stack[max_stack_size];
                uint8_t stack_size = 0;
                OptimisticGuard<LeafNode> leaf(vmcache, traverse(key, stack, stack_size), worker_id);
                if ((leaf->n_keys == 0 || key > leaf->keys[leaf->n_keys - 1]) && leaf->next == INVALID_PAGE_ID)
                    return std::nullopt;
                // search the key within the leaf node
                size_t l = lowerBound<KeyType>(leaf->keys, leaf->n_keys, key);
                if (leaf->keys[l] != key)
                    return std::nullopt;
                return leaf->get(l);
            } catch (const OLRestartException&) { }
        }
    }

    PageId getRootPid() const {
        return root_pid;
    }

private:
    ExclusiveGuard<LeafNode> insertIntoLeaf(ExclusiveGuard<LeafNode> leaf, PageId* stack, size_t stack_size, KeyType key, ValueType value) {
        // search the key within the leaf node
        size_t l = lowerBound<KeyType>(leaf->keys, leaf->n_keys, key);
        if (l < leaf->n_keys && leaf->keys[l] == key) {
            throw std::runtime_error("Key already exists!");
        } else {
            // insert new key
            bool fence_key_update = false;
            if (key < leaf->lfence) {
                leaf->lfence = key;
                fence_key_update = true;
            }
            if (key >= leaf->rfence) {
                leaf->rfence = key + 1;
                fence_key_update = true;
            }
            if (leaf->n_keys == LeafNode::capacity) {
                // split leaf node
                ExclusiveGuard<LeafNode> new_leaf(vmcache, vmcache.allocatePage(), worker_id);
                new_leaf->level = 0;
                new_leaf->next = leaf->next;
                leaf->next = new_leaf.pid;
                auto separator = leaf->split(new_leaf);
                leaf->rfence = separator;
                new_leaf->lfence = separator;
                new_leaf->rfence = leaf->rfence;
                assert(stack_size > 0);
                insertIntoInner(stack[stack_size - 1], stack, stack_size - 1, separator, new_leaf.pid, leaf->lfence, new_leaf->rfence);
                // setup for actual key insertion
                if (l > leaf->n_keys) {
                    // key goes into the new leaf node
                    l -= leaf->n_keys;
                    std::swap(new_leaf, leaf);
                }
            }
            if (fence_key_update) {
                // propagate fence key update down the stack
                size_t stack_i = stack_size;
                do {
                    PageId pid = stack[--stack_i];
                    ExclusiveGuard<InnerNode> inner(vmcache, pid, worker_id);
                    fence_key_update = false;
                    if (leaf->lfence < inner->lfence) {
                        inner->lfence = leaf->lfence;
                        fence_key_update = true;
                    }
                    if (leaf->rfence > inner->rfence) {
                        inner->rfence = leaf->rfence;
                        fence_key_update = true;
                    }
                } while (stack_i != 0 && fence_key_update);
            }
            // actual insert
            leaf->insert(l, key, value);
            return leaf;
        }
    }

    void insertIntoInner(PageId inner_pid, PageId* stack, size_t stack_size, KeyType key, PageId child, KeyType lfence, KeyType rfence) {
        ExclusiveGuard<InnerNode> inner(vmcache, inner_pid, worker_id);
        size_t l = lowerBound(inner->keys, inner->n_keys, key);
        if (inner->n_keys == InnerNode::capacity) {
            ExclusiveGuard<InnerNode> new_inner = splitInner(inner, stack, stack_size);
            // setup for actual key insertion
            if (l > inner->n_keys) {
                // key goes into the new inner node
                l -= inner->n_keys + 1;
                std::swap(new_inner, inner);
            }
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
        if (inner->lfence > lfence)
            inner->lfence = lfence;
        if (inner->rfence < rfence)
            inner->rfence = rfence;
    }

    ExclusiveGuard<InnerNode> splitInner(ExclusiveGuard<InnerNode>& inner, PageId* stack, size_t stack_size) {
        inner->n_keys = (InnerNode::capacity + 1) / 2;
        // inner = left node; new_inner = right node
        ExclusiveGuard<InnerNode> new_inner(vmcache, vmcache.allocatePage(), worker_id);
        new_inner->n_keys = InnerNode::capacity / 2 - 1;
        new_inner->level = inner->level;
        new_inner->lfence = inner->keys[inner->n_keys];
        new_inner->rfence = inner->rfence;
        inner->rfence = inner->keys[inner->n_keys];
        assert(inner->n_keys + new_inner->n_keys == InnerNode::capacity - 1);
        memcpy(new_inner->keys, inner->keys + inner->n_keys + 1, new_inner->n_keys * sizeof(KeyType));
        memcpy(new_inner->children, inner->children + inner->n_keys + 1, (new_inner->n_keys + 1) * sizeof(PageId));
        const KeyType split_key = inner->keys[inner->n_keys];
        if (stack_size == 0) {
            // we just split the root page, so we need to create a new 'inner', transfer its contents to 'inner' and update it to point only to 'inner' and 'new_inner'
            PageId root_pid = inner.pid;
            // copy 'root' to new 'inner'
            inner = AllocGuard<InnerNode>(vmcache, worker_id);
            ExclusiveGuard<InnerNode> root(vmcache, root_pid, worker_id);
            inner->level = root->level;
            inner->n_keys = root->n_keys;
            memcpy(inner->keys, root->keys, inner->n_keys * sizeof(KeyType));
            memcpy(inner->children, root->children, (inner->n_keys + 1) * sizeof(PageId));
            // recreate 'root'
            root->n_keys = 1;
            root->level = new_inner->level + 1;
            root->lfence = inner->lfence;
            root->rfence = new_inner->rfence;
            root->keys[0] = split_key;
            root->children[0] = inner.pid;
            root->children[1] = new_inner.pid;
        } else {
            PageId parent = stack[--stack_size];
            insertIntoInner(parent, stack, stack_size, split_key, new_inner.pid, inner->lfence, new_inner->rfence);
        }
        return new_inner;
    }

    PageId getFirstLeaf() const {
        for (size_t repeat_counter = 0; ; repeat_counter++) {
            try {
                OptimisticGuard<InnerNode> current(vmcache, root_pid, worker_id);
                while (true) {
                    if (current->level == 1) {
                        return current->children[0];
                    } else {
                        current = OptimisticGuard<InnerNode>(vmcache, current->children[0], worker_id);
                    }
                }
            } catch (const OLRestartException&) { }
        }
    }

    PageId getLastLeaf() const {
        for (size_t repeat_counter = 0; ; repeat_counter++) {
            try {
                OptimisticGuard<InnerNode> current(vmcache, root_pid, worker_id);
                while (true) {
                    if (current->level == 1) {
                        return current->children[current->n_keys];
                    } else {
                        current = OptimisticGuard<InnerNode>(vmcache, current->children[current->n_keys], worker_id);
                    }
                }
            } catch (const OLRestartException&) { }
        }
    }

    VMCache& vmcache;
    PageId root_pid;
    const uint32_t worker_id;
};