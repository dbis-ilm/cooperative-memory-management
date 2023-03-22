#include "db.hpp"

#include "types.hpp"
#include "../scheduling/execution_context.hpp"
#include "../execution/paged_vector_iterator.hpp"
#include "../storage/persistence/btree.hpp"
#include "../storage/persistence/root.hpp"
#include "../storage/persistence/column.hpp"
#include "../storage/persistence/table.hpp"
#include "../storage/policy/cache_partition.hpp"
#include "../utils/stringify.hpp"

DB::DB(size_t memory_limit, int fd, bool create, const size_t num_workers, bool stats_on_shutdown, std::unique_ptr<PartitioningStrategy>&& partitioning_strategy, size_t max_size_in_pages)
    : vmcache(memory_limit, max_size_in_pages, fd, std::move(partitioning_strategy), stats_on_shutdown, num_workers) {
    if (create) {
        // allocate root page
        PageId root_pid = vmcache.allocatePage();
        if (root_pid != ROOT_PID)
            throw std::runtime_error("Failed to allocate root page!");
        RootPage* root_page = reinterpret_cast<RootPage*>(vmcache.fixExclusive(ROOT_PID, 0));
        root_page->magic = ROOTPAGE_MAGIC;
        root_page->persistence_version = PERSISTENCE_VERSION;
        root_page->schema_catalog_basepage = createTableInternal(2, 0); // (schema_id int64, schema_name char(64))
        root_page->table_catalog_basepage = createTableInternal(4, 0); // (table_id int64, schema_id int64, table_name char(64), basepage_pid PageId)
        vmcache.unfixExclusive(ROOT_PID);
        default_schema_id = createSchema("SYSTEM", 0);
        if (default_schema_id != 0)
            throw std::runtime_error("Unexpected default schema id");
    } else {
        default_schema_id = 0;
        RootPage* root_page = reinterpret_cast<RootPage*>(vmcache.fixShared(ROOT_PID, 0));
        if (root_page->magic != ROOTPAGE_MAGIC)
            throw std::runtime_error("Detected invalid root page");
        if (root_page->persistence_version != PERSISTENCE_VERSION)
            throw std::runtime_error("The persistence version is incompatible, please recreate the database");
        vmcache.unfixShared(ROOT_PID);
    }
}

DB::DB(size_t memory_limit, int fd, bool create, const size_t num_workers, bool stats_on_shutdown, size_t max_size_in_pages)
    : DB(memory_limit, fd, create, num_workers, stats_on_shutdown, std::make_unique<BasicPartitioningStrategy<ClockEvictionCachePartition>>(), max_size_in_pages) { }

#define MAX_DB_OBJECT_NAME_LENGTH 64ul
#define SCHEMA_SCHEMA_ID_CID 0
#define SCHEMA_SCHEMA_NAME_CID 1
#define TABLE_TABLE_ID_CID 0
#define TABLE_SCHEMA_ID_CID 1
#define TABLE_TABLE_NAME_CID 2
#define TABLE_BASEPAGE_PID_CID 3

uint64_t DB::createSchema(const std::string& schema_name, uint32_t worker_id) {
    if (schema_name.size() > MAX_DB_OBJECT_NAME_LENGTH)
        throw std::runtime_error("'schema_name' exceeds the maximum length for database objects of " stringify(MAX_DB_OBJECT_NAME_LENGTH) " bytes");
    PageId schema_table_pid = reinterpret_cast<RootPage*>(vmcache.fixShared(ROOT_PID, worker_id))->schema_catalog_basepage;
    vmcache.unfixShared(ROOT_PID);
    TableBasepage* schema_basepage = reinterpret_cast<TableBasepage*>(vmcache.fixExclusive(schema_table_pid, worker_id));
    uint64_t schema_id = schema_basepage->cardinality;
    appendFixedSizeValue(schema_basepage->cardinality, schema_basepage->column_basepages[SCHEMA_SCHEMA_ID_CID], &schema_id, sizeof(uint64_t), worker_id);
    char schema_name_padded[MAX_DB_OBJECT_NAME_LENGTH] = {};
    memcpy(schema_name_padded, schema_name.c_str(), schema_name.size());
    appendFixedSizeValue(schema_basepage->cardinality, schema_basepage->column_basepages[SCHEMA_SCHEMA_NAME_CID], schema_name_padded, MAX_DB_OBJECT_NAME_LENGTH, worker_id);
    schema_basepage->cardinality++;
    vmcache.unfixExclusive(schema_table_pid);
    return schema_id;
}

uint64_t DB::createTable(uint64_t schema_id, const std::string& table_name, size_t num_columns, uint32_t worker_id) {
    if (table_name.size() > MAX_DB_OBJECT_NAME_LENGTH)
        throw std::runtime_error("'table_name' exceeds the maximum length for database objects of " stringify(MAX_DB_OBJECT_NAME_LENGTH) " bytes");
    // TODO: integrity check on schema_id
    PageId table_table_pid = reinterpret_cast<RootPage*>(vmcache.fixShared(ROOT_PID, worker_id))->table_catalog_basepage;
    vmcache.unfixShared(ROOT_PID);
    TableBasepage* table_basepage = reinterpret_cast<TableBasepage*>(vmcache.fixExclusive(table_table_pid, worker_id));
    uint64_t table_id = table_basepage->cardinality;
    appendFixedSizeValue(table_basepage->cardinality, table_basepage->column_basepages[TABLE_TABLE_ID_CID], &table_id, sizeof(uint64_t), worker_id);
    appendFixedSizeValue(table_basepage->cardinality, table_basepage->column_basepages[TABLE_SCHEMA_ID_CID], &schema_id, sizeof(uint64_t), worker_id);
    char table_name_padded[MAX_DB_OBJECT_NAME_LENGTH] = {};
    memcpy(table_name_padded, table_name.c_str(), table_name.size());
    appendFixedSizeValue(table_basepage->cardinality, table_basepage->column_basepages[TABLE_TABLE_NAME_CID], table_name_padded, MAX_DB_OBJECT_NAME_LENGTH, worker_id);
    uint64_t basepage_pid = static_cast<uint64_t>(createTableInternal(num_columns, worker_id));
    appendFixedSizeValue(table_basepage->cardinality, table_basepage->column_basepages[TABLE_BASEPAGE_PID_CID], &basepage_pid, sizeof(uint64_t), worker_id);
    table_basepage->cardinality++;
    vmcache.unfixExclusive(table_table_pid);

    return table_id;
}

template <size_t n>
void createCompositePrimaryKeyIndex(VMCache& vmcache, PageId table_basepage_pid, const ExecutionContext context) {
    TableBasepage* table_basepage = reinterpret_cast<TableBasepage*>(vmcache.fixExclusive(table_basepage_pid, context.getWorkerId()));
    BTree<CompositeKey<n>, size_t> index(vmcache, context.getWorkerId());
    table_basepage->primary_key_index_basepage = index.getRootPid();
    const size_t cardinality = table_basepage->cardinality;
    std::vector<PagedVectorIterator<uint32_t>> key_its;
    key_its.reserve(n);
    for (size_t i = 0; i < n; i++) {
        key_its.emplace_back(vmcache, table_basepage->column_basepages[i], 0, context.getWorkerId());
    }
    vmcache.unfixExclusive(table_basepage_pid);

    for (size_t j = 0; j < cardinality; j++) {
        CompositeKey<n> key;
        for (size_t i = 0; i < n; i++) {
            key.keys[i] = *key_its[i];
            ++key_its[i];
        }
        index.insert(key, j);
    }
}

void DB::createPrimaryKeyIndex(uint64_t tid, size_t num_columns, const ExecutionContext context) {
    if (num_columns > 4)
        throw std::runtime_error("Currently only primary key indices on up to four columns are supported");
    PageId table_basepage_pid = getTableBasepageId(tid, context.getWorkerId());

    switch (num_columns) {
        case 1:
            createCompositePrimaryKeyIndex<1>(vmcache, table_basepage_pid, context);
            break;
        case 2:
            createCompositePrimaryKeyIndex<2>(vmcache, table_basepage_pid, context);
            break;
        case 3:
            createCompositePrimaryKeyIndex<3>(vmcache, table_basepage_pid, context);
            break;
        case 4:
            createCompositePrimaryKeyIndex<4>(vmcache, table_basepage_pid, context);
            break;
    }
}

PageId DB::createTableInternal(size_t num_columns, uint32_t worker_id) {
    if (num_columns > (PAGE_SIZE - sizeof(TableBasepage)) / sizeof(PageId)) {
        throw std::runtime_error("'num_columns' exceeds maximum number of columns per table");
    }

    // allocate table basepage
    PageId pid = vmcache.allocatePage();
    TableBasepage* basepage = reinterpret_cast<TableBasepage*>(vmcache.fixExclusive(pid, worker_id));
    assert(basepage);
    basepage->primary_key_index_basepage = INVALID_PAGE_ID;

    // allocate column basepages
    for (size_t i = 0; i < num_columns; i++) {
        PageId col_pid = vmcache.allocatePage();
        basepage->column_basepages[i] = col_pid;
    }

    vmcache.unfixExclusive(pid);
    return pid;
}

class ColumnHelper {
    public:
        ColumnHelper(DB& db, PageId base, uint32_t worker_id) : db(db), base(base), worker_id(worker_id) {}

        PageId getPageId(size_t i) {
            const size_t data_pages_per_basepage = (PAGE_SIZE - sizeof(ColumnBasepage)) / sizeof(PageId);
            size_t basepage_i = i / data_pages_per_basepage;
            size_t basepage_off = i % data_pages_per_basepage;
            size_t current = 0;
            PageId pid = base;
            while (current != basepage_i) {
                PageId old_pid = pid;
                pid = reinterpret_cast<ColumnBasepage*>(db.vmcache.fixShared(pid, worker_id))->next;
                db.vmcache.unfixShared(old_pid);
                current++;
            }
            {
                PageId result = reinterpret_cast<ColumnBasepage*>(db.vmcache.fixShared(pid, worker_id))->data_pages[basepage_off];
                db.vmcache.unfixShared(pid);
                return result;
            }
        }

        void setPage(size_t i, PageId value) {
            const size_t data_pages_per_basepage = (PAGE_SIZE - sizeof(ColumnBasepage)) / sizeof(PageId);
            size_t basepage_i = i / data_pages_per_basepage;
            size_t basepage_off = i % data_pages_per_basepage;
            size_t current = 0;
            PageId pid = base;
            while (current != basepage_i) {
                PageId old_pid = pid;
                ColumnBasepage* bp = reinterpret_cast<ColumnBasepage*>(db.vmcache.fixExclusive(pid, worker_id));
                pid = bp->next;
                if (pid == 0) { // need to allocate the new basepage
                    bp->next = db.vmcache.allocatePage();
                    pid = bp->next;
                }
                db.vmcache.unfixExclusive(old_pid);
                current++;
            }
            reinterpret_cast<ColumnBasepage*>(db.vmcache.fixExclusive(pid, worker_id))->data_pages[basepage_off] = value;
            db.vmcache.unfixExclusive(pid);
        }

    private:
        DB& db;
        PageId base;
        uint32_t worker_id;
};

template <typename T>
void DB::appendValues(size_t existing_rows, PageId column_base, typename std::vector<T>::iterator begin, typename std::vector<T>::iterator end, uint32_t worker_id) {
    const size_t values_per_page = PAGE_SIZE / sizeof(T);
    size_t filled_values = existing_rows % values_per_page;
    size_t current_page_i = existing_rows / values_per_page;
    ColumnHelper helper(*this, column_base, worker_id);
    while (begin < end) {
        PageId pid;
        if (filled_values == 0) {
            pid = vmcache.allocatePage();
            helper.setPage(current_page_i, pid);
        } else {
            pid = helper.getPageId(current_page_i);
        }
        T* page = reinterpret_cast<T*>(vmcache.fixExclusive(pid, worker_id));
        assert(page);
        size_t value_count = std::min<size_t>(values_per_page - filled_values, end - begin);
        std::memcpy(page + filled_values, std::addressof(*begin), value_count * sizeof(T));
        vmcache.unfixExclusive(pid);
        begin += value_count;
        filled_values = 0;
        current_page_i++;
    }
}

void DB::appendFixedSizeValue(size_t existing_rows, PageId column_base, void* value, size_t len, uint32_t worker_id) {
    const size_t values_per_page = PAGE_SIZE / len;
    size_t filled_values = existing_rows % values_per_page;
    size_t current_page_i = existing_rows / values_per_page;
    ColumnHelper helper(*this, column_base, worker_id);
    PageId pid;
    if (filled_values == 0) {
        pid = vmcache.allocatePage();
        helper.setPage(current_page_i, pid);
    } else {
        pid = helper.getPageId(current_page_i);
    }
    char* page = reinterpret_cast<char*>(vmcache.fixExclusive(pid, worker_id));
    assert(page);
    memcpy(page + filled_values * len, value, len);
    vmcache.unfixExclusive(pid);
}

void DB::appendFixedSizeValues(size_t existing_rows, PageId column_base, void* values, size_t value_len, size_t num_values, uint32_t worker_id) {
    const size_t values_per_page = PAGE_SIZE / value_len;
    size_t filled_values = existing_rows % values_per_page;
    size_t current_page_i = existing_rows / values_per_page;
    ColumnHelper helper(*this, column_base, worker_id);
    size_t i = 0;
    while (i < num_values) {
        PageId pid;
        if (filled_values == 0) {
            pid = vmcache.allocatePage();
            helper.setPage(current_page_i, pid);
        } else {
            pid = helper.getPageId(current_page_i);
        }
        char* page = reinterpret_cast<char*>(vmcache.fixExclusive(pid, worker_id));
        assert(page);
        size_t value_count = std::min<size_t>(values_per_page - filled_values, num_values - i);
        std::memcpy(page + filled_values * value_len, reinterpret_cast<char*>(values) + i * value_len, value_count * value_len);
        vmcache.unfixExclusive(pid);
        filled_values = 0;
        current_page_i++;
        i += value_count;
    }
}

#define INSTANTIATE_APPEND_VALUES(type) \
template void DB::appendValues<type>(size_t, PageId, std::vector<type>::iterator, std::vector<type>::iterator, uint32_t);

// TODO: change to Decimal (will require some adjustments in the import code)
INSTANTIATE_APPEND_VALUES(int64_t); // for Decimal
INSTANTIATE_APPEND_VALUES(Identifier);
INSTANTIATE_APPEND_VALUES(Integer);

PageId DB::getTableBasepageId(uint64_t tid, uint32_t worker_id) {
    PageId table_table_pid = reinterpret_cast<RootPage*>(vmcache.fixShared(ROOT_PID, worker_id))->table_catalog_basepage;
    vmcache.unfixShared(ROOT_PID);
    TableBasepage* table_basepage = reinterpret_cast<TableBasepage*>(vmcache.fixShared(table_table_pid, worker_id));
    if (tid > table_basepage->cardinality)
        throw std::runtime_error("Invalid TID");
    PagedVectorIterator<uint64_t> it(vmcache, table_basepage->column_basepages[TABLE_BASEPAGE_PID_CID], tid, worker_id);
    vmcache.unfixShared(table_table_pid);
    return *it;
}

PageId DB::getTableBasepageId(const std::string& table_name, uint32_t worker_id) {
    if (table_name.size() > MAX_DB_OBJECT_NAME_LENGTH)
        throw std::runtime_error("'table_name' exceeds the maximum length for database objects of " stringify(MAX_DB_OBJECT_NAME_LENGTH) " bytes");
    PageId table_table_pid = reinterpret_cast<RootPage*>(vmcache.fixShared(ROOT_PID, worker_id))->table_catalog_basepage;
    vmcache.unfixShared(ROOT_PID);
    TableBasepage* table_basepage = reinterpret_cast<TableBasepage*>(vmcache.fixShared(table_table_pid, worker_id));
    GeneralPagedVectorIterator name_it(vmcache, table_basepage->column_basepages[TABLE_TABLE_NAME_CID], 0, MAX_DB_OBJECT_NAME_LENGTH, worker_id);
    const size_t cardinality = table_basepage->cardinality;
    const PageId pid_col_basepage = table_basepage->column_basepages[TABLE_BASEPAGE_PID_CID];
    vmcache.unfixShared(table_table_pid);
    for (size_t i = 0; i < cardinality; i++) {
        if (memcmp(name_it.getCurrentValue(), table_name.c_str(), std::min(table_name.size() + 1, MAX_DB_OBJECT_NAME_LENGTH)) == 0) {
            PagedVectorIterator<uint64_t> pid_it(vmcache, pid_col_basepage, i, worker_id);
            return *pid_it;
        }
        ++name_it;
    }
    throw std::runtime_error("Invalid 'table_name'");
}