#include "db.hpp"

#include "types.hpp"
#include "../scheduling/execution_context.hpp"
#include "../execution/paged_vector_iterator.hpp"
#include "../storage/guard.hpp"
#include "../storage/persistence/btree.hpp"
#include "../storage/persistence/root.hpp"
#include "../storage/persistence/column.hpp"
#include "../storage/persistence/table.hpp"
#include "../storage/policy/cache_partition.hpp"
#include "../utils/stringify.hpp"

DB::DB(size_t memory_limit, const std::string& path, bool sandbox, bool no_dirty_writeback, bool flush_asynchronously, bool use_eviction_target, const size_t num_workers, bool use_exmap, bool stats_on_shutdown, std::unique_ptr<PartitioningStrategy>&& partitioning_strategy, size_t max_size_in_pages)
    : vmcache(memory_limit, max_size_in_pages, path, sandbox, no_dirty_writeback, flush_asynchronously, use_eviction_target, std::move(partitioning_strategy), use_exmap, stats_on_shutdown, num_workers) {
    if (vmcache.isEmpty()) {
        std::cout << "Creating new database..." << std::endl;
        // allocate root page
        AllocGuard<RootPage> root_page(vmcache, 0);
        if (root_page.pid != ROOT_PID)
            throw std::runtime_error("Failed to allocate root page!");
        root_page->magic = ROOTPAGE_MAGIC;
        root_page->persistence_version = PERSISTENCE_VERSION;
        root_page->schema_catalog_basepage = createTableInternal(2, 0); // (schema_id int64, schema_name char(64))
        root_page->table_catalog_basepage = createTableInternal(4, 0); // (table_id int64, schema_id int64, table_name char(64), basepage_pid PageId)
        root_page.release();
        default_schema_id = createSchema("SYSTEM", 0);
        if (default_schema_id != 0)
            throw std::runtime_error("Unexpected default schema id");
    } else {
        default_schema_id = 0;
        SharedGuard<RootPage> root_page(vmcache, ROOT_PID, 0);
        if (root_page->magic != ROOTPAGE_MAGIC)
            throw std::runtime_error("Detected invalid root page");
        if (root_page->persistence_version != PERSISTENCE_VERSION)
            throw std::runtime_error("The persistence version is incompatible, please recreate the database");
    }
}

DB::DB(size_t memory_limit, const std::string& path, bool sandbox, bool no_dirty_writeback, bool flush_asynchronously, bool use_eviction_target, const size_t num_workers, bool use_exmap, bool stats_on_shutdown, size_t max_size_in_pages)
    : DB(memory_limit, path, sandbox, no_dirty_writeback, flush_asynchronously, use_eviction_target, num_workers, use_exmap, stats_on_shutdown, std::make_unique<BasicPartitioningStrategy<ClockEvictionCachePartition>>(), max_size_in_pages) { }

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
    PageId schema_table_pid = SharedGuard<RootPage>(vmcache, ROOT_PID, worker_id)->schema_catalog_basepage;
    ExclusiveGuard<TableBasepage> schema_basepage(vmcache, schema_table_pid, worker_id);
    BTree<RowId, bool> visibility(vmcache, schema_basepage->visibility_basepage, worker_id);
    auto insert_guard = visibility.insertNext(true);
    appendFixedSizeValue(insert_guard.key, schema_basepage->column_basepages[SCHEMA_SCHEMA_ID_CID], &insert_guard.key, sizeof(uint64_t), worker_id);
    char schema_name_padded[MAX_DB_OBJECT_NAME_LENGTH] = {};
    memcpy(schema_name_padded, schema_name.c_str(), schema_name.size());
    appendFixedSizeValue(insert_guard.key, schema_basepage->column_basepages[SCHEMA_SCHEMA_NAME_CID], schema_name_padded, MAX_DB_OBJECT_NAME_LENGTH, worker_id);
    return insert_guard.key;
}

uint64_t DB::createTable(uint64_t schema_id, const std::string& table_name, size_t num_columns, uint32_t worker_id) {
    if (table_name.size() > MAX_DB_OBJECT_NAME_LENGTH)
        throw std::runtime_error("'table_name' exceeds the maximum length for database objects of " stringify(MAX_DB_OBJECT_NAME_LENGTH) " bytes");
    // TODO: integrity check on schema_id
    PageId table_table_pid = SharedGuard<RootPage>(vmcache, ROOT_PID, worker_id)->table_catalog_basepage;
    ExclusiveGuard<TableBasepage> table_basepage(vmcache, table_table_pid, worker_id);
    BTree<RowId, bool> visibility(vmcache, table_basepage->visibility_basepage, worker_id);
    auto insert_guard = visibility.insertNext(true);
    appendFixedSizeValue(insert_guard.key, table_basepage->column_basepages[TABLE_TABLE_ID_CID], &insert_guard.key, sizeof(uint64_t), worker_id);
    appendFixedSizeValue(insert_guard.key, table_basepage->column_basepages[TABLE_SCHEMA_ID_CID], &schema_id, sizeof(uint64_t), worker_id);
    char table_name_padded[MAX_DB_OBJECT_NAME_LENGTH] = {};
    memcpy(table_name_padded, table_name.c_str(), table_name.size());
    appendFixedSizeValue(insert_guard.key, table_basepage->column_basepages[TABLE_TABLE_NAME_CID], table_name_padded, MAX_DB_OBJECT_NAME_LENGTH, worker_id);
    uint64_t basepage_pid = static_cast<uint64_t>(createTableInternal(num_columns, worker_id));
    appendFixedSizeValue(insert_guard.key, table_basepage->column_basepages[TABLE_BASEPAGE_PID_CID], &basepage_pid, sizeof(uint64_t), worker_id);

    return insert_guard.key;
}

template <size_t n>
void createCompositePrimaryKeyIndex(VMCache& vmcache, ExclusiveGuard<TableBasepage>& table_basepage, const ExecutionContext context) {
    BTree<CompositeKey<n>, size_t> index(vmcache, context.getWorkerId());
    table_basepage->primary_key_index_basepage = index.getRootPid();
    BTree<RowId, bool> visibility(vmcache, table_basepage->visibility_basepage, context.getWorkerId());
    std::vector<PagedVectorIterator<uint32_t>> key_its;
    key_its.reserve(n);
    for (size_t i = 0; i < n; i++) {
        key_its.emplace_back(vmcache, table_basepage->column_basepages[i], 0, context.getWorkerId());
    }
    table_basepage.release();

    for (auto vis_it = visibility.begin(); vis_it != visibility.end(); ++vis_it) {
        auto vis = *vis_it;
        if (!vis.second)
            continue;
        CompositeKey<n> key;
        for (size_t i = 0; i < n; i++) {
            key_its[i].reposition(vis.first);
            key.keys[i] = *key_its[i];
        }
        index.insert(key, vis.first);
    }
}

void DB::createPrimaryKeyIndex(const std::string& table_name, size_t num_columns, const ExecutionContext context) {
    if (num_columns > 4)
        throw std::runtime_error("Currently only primary key indices on up to four columns are supported");
    ExclusiveGuard<TableBasepage> table_basepage(vmcache, getTableBasepageId(table_name, context.getWorkerId()), context.getWorkerId());

    switch (num_columns) {
        case 1:
            createCompositePrimaryKeyIndex<1>(vmcache, table_basepage, context);
            break;
        case 2:
            createCompositePrimaryKeyIndex<2>(vmcache, table_basepage, context);
            break;
        case 3:
            createCompositePrimaryKeyIndex<3>(vmcache, table_basepage, context);
            break;
        case 4:
            createCompositePrimaryKeyIndex<4>(vmcache, table_basepage, context);
            break;
    }
}

PageId DB::createTableInternal(size_t num_columns, uint32_t worker_id) {
    if (num_columns > (PAGE_SIZE - sizeof(TableBasepage)) / sizeof(PageId)) {
        throw std::runtime_error("'num_columns' exceeds maximum number of columns per table");
    }

    // allocate table basepage
    AllocGuard<TableBasepage> basepage(vmcache, worker_id);
    basepage->primary_key_index_basepage = INVALID_PAGE_ID;

    // create visibility B+-Tree
    BTree<RowId, bool> visibility(vmcache, worker_id);
    basepage->visibility_basepage = visibility.getRootPid();

    // allocate column basepages
    for (size_t i = 0; i < num_columns; i++) {
        PageId col_pid = vmcache.allocatePage();
        basepage->column_basepages[i] = col_pid;
    }

    return basepage.pid;
}

class ColumnHelper {
    public:
        ColumnHelper(DB& db, PageId base, uint32_t worker_id) : db(db), base(base), worker_id(worker_id) {}

        PageId getPageId(size_t i) {
            auto it = db.append_pids.find(base);
            if (it == db.append_pids.end()) {
                const size_t data_pages_per_basepage = (PAGE_SIZE - sizeof(ColumnBasepage)) / sizeof(PageId);
                size_t basepage_i = i / data_pages_per_basepage;
                size_t basepage_off = i % data_pages_per_basepage;
                size_t current = 0;
                PageId pid = base;
                while (current != basepage_i) {
                    pid = SharedGuard<ColumnBasepage>(db.vmcache, pid, worker_id)->next;
                    current++;
                }
                PageId result = SharedGuard<ColumnBasepage>(db.vmcache, pid, worker_id)->data_pages[basepage_off];
                {
                    std::lock_guard<std::mutex> guard(db.append_pids_mutex);
                    db.append_pids.insert(std::make_pair(base, result));
                }
                return result;
            }
            PageId result = it->second;
            return result;
        }

        void setPage(size_t i, PageId value) {
            const size_t data_pages_per_basepage = (PAGE_SIZE - sizeof(ColumnBasepage)) / sizeof(PageId);
            size_t basepage_i = i / data_pages_per_basepage;
            size_t basepage_off = i % data_pages_per_basepage;
            size_t current = 0;
            PageId pid = base;
            while (current != basepage_i) {
                ExclusiveGuard<ColumnBasepage> bp(db.vmcache, pid, worker_id);
                pid = bp->next;
                if (pid == 0) { // need to allocate the new basepage
                    bp->next = db.vmcache.allocatePage();
                    pid = bp->next;
                }
                current++;
            }
            ExclusiveGuard<ColumnBasepage>(db.vmcache, pid, worker_id)->data_pages[basepage_off] = value;
            {
                std::lock_guard<std::mutex> guard(db.append_pids_mutex);
                db.append_pids.insert_or_assign(base, value);
            }
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
        ExclusiveGuard<ColumnDataPage> page(vmcache, pid, worker_id);
        size_t value_count = std::min<size_t>(values_per_page - filled_values, end - begin);
        std::memcpy(page->data + filled_values * sizeof(T), std::addressof(*begin), value_count * sizeof(T));
        begin += value_count;
        filled_values = 0;
        current_page_i++;
    }
}

void DB::appendFixedSizeValue(size_t existing_rows, PageId column_base, const void* value, size_t len, uint32_t worker_id) {
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
    ExclusiveGuard<ColumnDataPage> page(vmcache, pid, worker_id);
    memcpy(page->data + filled_values * len, value, len);
}

void DB::appendFixedSizeValues(size_t existing_rows, PageId column_base, const void* values, size_t value_len, size_t num_values, uint32_t worker_id) {
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
        ExclusiveGuard<ColumnDataPage> page(vmcache, pid, worker_id);
        size_t value_count = std::min<size_t>(values_per_page - filled_values, num_values - i);
        std::memcpy(page->data + filled_values * value_len, reinterpret_cast<const char*>(values) + i * value_len, value_count * value_len);
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

size_t DB::getNumTables(uint32_t worker_id) {
    PageId table_table_pid = SharedGuard<RootPage>(vmcache, ROOT_PID, worker_id)->table_catalog_basepage;
    BTree<RowId, bool> visibility(vmcache, SharedGuard<TableBasepage>(vmcache, table_table_pid, worker_id)->visibility_basepage, worker_id);
    size_t result = 0;
    for (auto it = visibility.begin(); it != visibility.end(); ++it) {
        if ((*it).second)
            result++;
    }
    return result;
}

PageId DB::getTableBasepageId(uint64_t tid, uint32_t worker_id) {
    PageId table_table_pid = SharedGuard<RootPage>(vmcache, ROOT_PID, worker_id)->table_catalog_basepage;
    SharedGuard<TableBasepage> table_basepage(vmcache, table_table_pid, worker_id);
    BTree<RowId, bool> visibility(vmcache, table_basepage->visibility_basepage, worker_id);
    if (!visibility.lookupValue(tid).value_or(false))
        throw std::runtime_error("Invalid TID");
    PagedVectorIterator<uint64_t> it(vmcache, table_basepage->column_basepages[TABLE_BASEPAGE_PID_CID], tid, worker_id);
    return *it;
}

PageId DB::getTableBasepageId(const std::string& table_name, uint32_t worker_id) {
    if (table_name.size() > MAX_DB_OBJECT_NAME_LENGTH)
        throw std::runtime_error("'table_name' exceeds the maximum length for database objects of " stringify(MAX_DB_OBJECT_NAME_LENGTH) " bytes");
    PageId table_table_pid = SharedGuard<RootPage>(vmcache, ROOT_PID, worker_id)->table_catalog_basepage;
    SharedGuard<TableBasepage> table_basepage(vmcache, table_table_pid, worker_id);
    BTree<RowId, bool> visibility(vmcache, table_basepage->visibility_basepage, worker_id);
    GeneralPagedVectorIterator name_it(vmcache, table_basepage->column_basepages[TABLE_TABLE_NAME_CID], 0, MAX_DB_OBJECT_NAME_LENGTH, worker_id);
    const PageId pid_col_basepage = table_basepage->column_basepages[TABLE_BASEPAGE_PID_CID];
    table_basepage.release();
    for (auto v_it = visibility.begin(); v_it != visibility.end(); ++v_it) {
        if ((*v_it).second) {
            name_it.reposition((*v_it).first);
            if(memcmp(name_it.getCurrentValue(), table_name.c_str(), std::min(table_name.size() + 1, MAX_DB_OBJECT_NAME_LENGTH)) == 0) {
                PagedVectorIterator<uint64_t> pid_it(vmcache, pid_col_basepage, (*v_it).first, worker_id);
                return *pid_it;
            }
        }
    }
    throw std::runtime_error("Invalid 'table_name'");
}