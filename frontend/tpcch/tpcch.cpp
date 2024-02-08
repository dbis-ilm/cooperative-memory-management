#include "tpcch.hpp"

#include <map>

#include "prototype/execution/csv_import_pipeline.hpp"
#include "prototype/execution/paged_vector_iterator.hpp"
#include "prototype/execution/qep.hpp"
#include "prototype/storage/persistence/btree.hpp"
#include "prototype/storage/persistence/table.hpp"

namespace tpcch {

void createTables(DB& db, const ExecutionContext context) {
    db.createTable(db.default_schema_id, "WAREHOUSE", 9, context.getWorkerId());
    db.createTable(db.default_schema_id, "DISTRICT", 11, context.getWorkerId());
    db.createTable(db.default_schema_id, "CUSTOMER", 22, context.getWorkerId());
    db.createTable(db.default_schema_id, "HISTORY", 8, context.getWorkerId());
    db.createTable(db.default_schema_id, "NEWORDER", 3, context.getWorkerId());
    db.createTable(db.default_schema_id, "ORDER", 8, context.getWorkerId());
    db.createTable(db.default_schema_id, "ORDERLINE", 10, context.getWorkerId());
    db.createTable(db.default_schema_id, "ITEM", 5, context.getWorkerId());
    db.createTable(db.default_schema_id, "STOCK", 18, context.getWorkerId());
    db.createTable(db.default_schema_id, "NATION", 4, context.getWorkerId());
    db.createTable(db.default_schema_id, "SUPPLIER", 7, context.getWorkerId());
    db.createTable(db.default_schema_id, "REGION", 3, context.getWorkerId());
}

void createIndexes(DB& db, const ExecutionContext context) {
    db.createPrimaryKeyIndex("WAREHOUSE", 1, context);
    db.createPrimaryKeyIndex("DISTRICT", 2, context);
    db.createPrimaryKeyIndex("CUSTOMER", 3, context);
    db.createPrimaryKeyIndex("NEWORDER", 3, context);
    db.createPrimaryKeyIndex("ORDER", 3, context);
    db.createPrimaryKeyIndex("ORDERLINE", 4, context);
    db.createPrimaryKeyIndex("ITEM", 1, context);
    db.createPrimaryKeyIndex("STOCK", 2, context);
    db.createPrimaryKeyIndex("NATION", 1, context);
    db.createPrimaryKeyIndex("SUPPLIER", 1, context);
    db.createPrimaryKeyIndex("REGION", 1, context);

    // ORDER DWC index
    ExclusiveGuard<TableBasepage> order_basepage(db.vmcache, db.getTableBasepageId("ORDER", context.getWorkerId()), context.getWorkerId());
    BTree<CompositeKey<4>, size_t> index(db.vmcache, context.getWorkerId());
    order_basepage->additional_index_basepage = index.getRootPid();
    BTree<RowId, bool> visibility(db.vmcache, order_basepage->visibility_basepage, context.getWorkerId());
    std::vector<PagedVectorIterator<uint32_t>> key_its;
    key_its.reserve(4);
    for (size_t i = 0; i < 4; i++) {
        key_its.emplace_back(db.vmcache, order_basepage->column_basepages[i], 0, context.getWorkerId());
    }
    order_basepage.release();

    for (auto vis_it = visibility.begin(); vis_it != visibility.end(); ++vis_it) {
        auto vis = *vis_it;
        if (!vis.second)
            continue;
        for (size_t i = 0; i < 4; i++)
            key_its[i].reposition(vis.first);
        CompositeKey<4> key { *key_its[0], *key_its[1], *key_its[3], *key_its[2] };
        index.insert(key, vis.first);
    }
}

std::string joinPath(const std::string& a, const std::string& b) {
    std::stringstream path_ss;
    path_ss << a;
    if (a.back() != '/')
        path_ss << "/";
    path_ss << b;
    return path_ss.str();
}

class UnexpectedCardinalityError : public std::exception {
public:
    UnexpectedCardinalityError(const std::string& object, size_t cardinality, size_t expected_cardinality) {
        std::stringstream err_msg_strstr;
        err_msg_strstr << object << " has cardinality " << cardinality << ", expected " << expected_cardinality;
        err_msg = err_msg_strstr.str();
    }

    const char* what() const noexcept override { return err_msg.c_str(); }

private:
    std::string err_msg;
};

const char CSV_SEP = '|';

void importFromCSV(DB& db, const std::string& path, const ExecutionContext context) {
    std::vector<std::unique_ptr<ExecutablePipeline>> pipelines;
    // WAREHOUSE
    std::unordered_map<size_t, CSVColumnSpec> warehouse_column_spec;
    ExclusiveGuard<TableBasepage> warehouse_basepage(db.vmcache, db.getTableBasepageId("WAREHOUSE", context.getWorkerId()), context.getWorkerId());
    warehouse_column_spec.insert({0, CSVColumnSpec(ParseTypeDescription::Int32(), warehouse_basepage->column_basepages[W_ID_CID])});
    warehouse_column_spec.insert({1, CSVColumnSpec(ParseTypeDescription::Char(10), warehouse_basepage->column_basepages[W_NAME_CID])});
    warehouse_column_spec.insert({2, CSVColumnSpec(ParseTypeDescription::Char(20), warehouse_basepage->column_basepages[W_STREET_1_CID])});
    warehouse_column_spec.insert({3, CSVColumnSpec(ParseTypeDescription::Char(20), warehouse_basepage->column_basepages[W_STREET_2_CID])});
    warehouse_column_spec.insert({4, CSVColumnSpec(ParseTypeDescription::Char(20), warehouse_basepage->column_basepages[W_CITY_CID])});
    warehouse_column_spec.insert({5, CSVColumnSpec(ParseTypeDescription::Char(2), warehouse_basepage->column_basepages[W_STATE_CID])});
    warehouse_column_spec.insert({6, CSVColumnSpec(ParseTypeDescription::Char(9), warehouse_basepage->column_basepages[W_ZIP_CID])});
    warehouse_column_spec.insert({7, CSVColumnSpec(ParseTypeDescription::Decimal(4), warehouse_basepage->column_basepages[W_TAX_CID])});
    warehouse_column_spec.insert({8, CSVColumnSpec(ParseTypeDescription::Decimal(2), warehouse_basepage->column_basepages[W_YTD_CID])});
    pipelines.push_back(std::make_unique<CSVImportPipeline>(0, std::ref(db), joinPath(path, std::string("WAREHOUSE.tbl")), CSV_SEP, warehouse_column_spec, 9, warehouse_basepage->visibility_basepage));

    // DISTRICT
    std::unordered_map<size_t, CSVColumnSpec> district_column_spec;
    ExclusiveGuard<TableBasepage> district_basepage(db.vmcache, db.getTableBasepageId("DISTRICT", context.getWorkerId()), context.getWorkerId());
    district_column_spec.insert({0, CSVColumnSpec(ParseTypeDescription::Int32(), district_basepage->column_basepages[D_ID_CID])});
    district_column_spec.insert({1, CSVColumnSpec(ParseTypeDescription::Int32(), district_basepage->column_basepages[D_W_ID_CID])});
    district_column_spec.insert({2, CSVColumnSpec(ParseTypeDescription::Char(10), district_basepage->column_basepages[D_NAME_CID])});
    district_column_spec.insert({3, CSVColumnSpec(ParseTypeDescription::Char(20), district_basepage->column_basepages[D_STREET_1_CID])});
    district_column_spec.insert({4, CSVColumnSpec(ParseTypeDescription::Char(20), district_basepage->column_basepages[D_STREET_2_CID])});
    district_column_spec.insert({5, CSVColumnSpec(ParseTypeDescription::Char(20), district_basepage->column_basepages[D_CITY_CID])});
    district_column_spec.insert({6, CSVColumnSpec(ParseTypeDescription::Char(2), district_basepage->column_basepages[D_STATE_CID])});
    district_column_spec.insert({7, CSVColumnSpec(ParseTypeDescription::Char(9), district_basepage->column_basepages[D_ZIP_CID])});
    district_column_spec.insert({8, CSVColumnSpec(ParseTypeDescription::Decimal(4), district_basepage->column_basepages[D_TAX_CID])});
    district_column_spec.insert({9, CSVColumnSpec(ParseTypeDescription::Decimal(2), district_basepage->column_basepages[D_YTD_CID])});
    district_column_spec.insert({10, CSVColumnSpec(ParseTypeDescription::Int32(), district_basepage->column_basepages[D_NEXT_O_ID_CID])});
    pipelines.push_back(std::make_unique<CSVImportPipeline>(1, std::ref(db), joinPath(path, std::string("DISTRICT.tbl")), CSV_SEP, district_column_spec, 11, district_basepage->visibility_basepage));
    pipelines.back()->addDependency(pipelines.size() - 2);

    // CUSTOMER
    std::unordered_map<size_t, CSVColumnSpec> customer_column_spec;
    ExclusiveGuard<TableBasepage> customer_basepage(db.vmcache, db.getTableBasepageId("CUSTOMER", context.getWorkerId()), context.getWorkerId());
    customer_column_spec.insert({0, CSVColumnSpec(ParseTypeDescription::Int32(), customer_basepage->column_basepages[C_ID_CID])});
    customer_column_spec.insert({1, CSVColumnSpec(ParseTypeDescription::Int32(), customer_basepage->column_basepages[C_D_ID_CID])});
    customer_column_spec.insert({2, CSVColumnSpec(ParseTypeDescription::Int32(), customer_basepage->column_basepages[C_W_ID_CID])});
    customer_column_spec.insert({3, CSVColumnSpec(ParseTypeDescription::Char(16), customer_basepage->column_basepages[C_FIRST_CID])});
    customer_column_spec.insert({4, CSVColumnSpec(ParseTypeDescription::Char(2), customer_basepage->column_basepages[C_MIDDLE_CID])});
    customer_column_spec.insert({5, CSVColumnSpec(ParseTypeDescription::Char(16), customer_basepage->column_basepages[C_LAST_CID])});
    customer_column_spec.insert({6, CSVColumnSpec(ParseTypeDescription::Char(20), customer_basepage->column_basepages[C_STREET_1_CID])});
    customer_column_spec.insert({7, CSVColumnSpec(ParseTypeDescription::Char(20), customer_basepage->column_basepages[C_STREET_2_CID])});
    customer_column_spec.insert({8, CSVColumnSpec(ParseTypeDescription::Char(20), customer_basepage->column_basepages[C_CITY_CID])});
    customer_column_spec.insert({9, CSVColumnSpec(ParseTypeDescription::Char(2), customer_basepage->column_basepages[C_STATE_CID])});
    customer_column_spec.insert({10, CSVColumnSpec(ParseTypeDescription::Char(9), customer_basepage->column_basepages[C_ZIP_CID])});
    customer_column_spec.insert({11, CSVColumnSpec(ParseTypeDescription::Char(16), customer_basepage->column_basepages[C_PHONE_CID])});
    customer_column_spec.insert({12, CSVColumnSpec(ParseTypeDescription::DateTime(), customer_basepage->column_basepages[C_SINCE_CID])});
    customer_column_spec.insert({13, CSVColumnSpec(ParseTypeDescription::Char(2), customer_basepage->column_basepages[C_CREDIT_CID])});
    customer_column_spec.insert({14, CSVColumnSpec(ParseTypeDescription::Decimal(2), customer_basepage->column_basepages[C_CREDIT_LIM_CID])});
    customer_column_spec.insert({15, CSVColumnSpec(ParseTypeDescription::Decimal(4), customer_basepage->column_basepages[C_DISCOUNT_CID])});
    customer_column_spec.insert({16, CSVColumnSpec(ParseTypeDescription::Decimal(2), customer_basepage->column_basepages[C_BALANCE_CID])});
    customer_column_spec.insert({17, CSVColumnSpec(ParseTypeDescription::Decimal(2), customer_basepage->column_basepages[C_YTD_PAYMENT_CID])});
    customer_column_spec.insert({18, CSVColumnSpec(ParseTypeDescription::Int32(), customer_basepage->column_basepages[C_PAYMENT_CNT_CID])});
    customer_column_spec.insert({19, CSVColumnSpec(ParseTypeDescription::Int32(), customer_basepage->column_basepages[C_DELIVERY_CNT_CID])});
    customer_column_spec.insert({20, CSVColumnSpec(ParseTypeDescription::Char(500), customer_basepage->column_basepages[C_DATA_CID])});
    customer_column_spec.insert({21, CSVColumnSpec(ParseTypeDescription::Int32(), customer_basepage->column_basepages[C_N_NATIONKEY_CID])});
    pipelines.push_back(std::make_unique<CSVImportPipeline>(2, std::ref(db), joinPath(path, std::string("CUSTOMER.tbl")), CSV_SEP, customer_column_spec, 22, customer_basepage->visibility_basepage));
    pipelines.back()->addDependency(pipelines.size() - 2);

    // HISTORY
    std::unordered_map<size_t, CSVColumnSpec> history_column_spec;
    ExclusiveGuard<TableBasepage> history_basepage(db.vmcache, db.getTableBasepageId("HISTORY", context.getWorkerId()), context.getWorkerId());
    history_column_spec.insert({0, CSVColumnSpec(ParseTypeDescription::Int32(), history_basepage->column_basepages[H_C_ID_CID])});
    history_column_spec.insert({1, CSVColumnSpec(ParseTypeDescription::Int32(), history_basepage->column_basepages[H_C_D_ID_CID])});
    history_column_spec.insert({2, CSVColumnSpec(ParseTypeDescription::Int32(), history_basepage->column_basepages[H_C_W_ID_CID])});
    history_column_spec.insert({3, CSVColumnSpec(ParseTypeDescription::Int32(), history_basepage->column_basepages[H_D_ID_CID])});
    history_column_spec.insert({4, CSVColumnSpec(ParseTypeDescription::Int32(), history_basepage->column_basepages[H_W_ID_CID])});
    history_column_spec.insert({5, CSVColumnSpec(ParseTypeDescription::DateTime(), history_basepage->column_basepages[H_DATE_CID])});
    history_column_spec.insert({6, CSVColumnSpec(ParseTypeDescription::Decimal(2), history_basepage->column_basepages[H_AMOUNT_CID])});
    history_column_spec.insert({7, CSVColumnSpec(ParseTypeDescription::Char(24), history_basepage->column_basepages[H_DATA_CID])});
    pipelines.push_back(std::make_unique<CSVImportPipeline>(3, std::ref(db), joinPath(path, std::string("HISTORY.tbl")), CSV_SEP, history_column_spec, 8, history_basepage->visibility_basepage));
    pipelines.back()->addDependency(pipelines.size() - 2);

    // NEWORDER
    std::unordered_map<size_t, CSVColumnSpec> neworder_column_spec;
    ExclusiveGuard<TableBasepage> neworder_basepage(db.vmcache, db.getTableBasepageId("NEWORDER", context.getWorkerId()), context.getWorkerId());
    neworder_column_spec.insert({0, CSVColumnSpec(ParseTypeDescription::Int32(), neworder_basepage->column_basepages[NO_O_ID_CID])});
    neworder_column_spec.insert({1, CSVColumnSpec(ParseTypeDescription::Int32(), neworder_basepage->column_basepages[NO_D_ID_CID])});
    neworder_column_spec.insert({2, CSVColumnSpec(ParseTypeDescription::Int32(), neworder_basepage->column_basepages[NO_W_ID_CID])});
    pipelines.push_back(std::make_unique<CSVImportPipeline>(4, std::ref(db), joinPath(path, std::string("NEWORDER.tbl")), CSV_SEP, neworder_column_spec, 3, neworder_basepage->visibility_basepage));
    pipelines.back()->addDependency(pipelines.size() - 2);

    // ORDER
    std::unordered_map<size_t, CSVColumnSpec> order_column_spec;
    ExclusiveGuard<TableBasepage> order_basepage(db.vmcache, db.getTableBasepageId("ORDER", context.getWorkerId()), context.getWorkerId());
    order_column_spec.insert({0, CSVColumnSpec(ParseTypeDescription::Int32(), order_basepage->column_basepages[O_ID_CID])});
    order_column_spec.insert({1, CSVColumnSpec(ParseTypeDescription::Int32(), order_basepage->column_basepages[O_D_ID_CID])});
    order_column_spec.insert({2, CSVColumnSpec(ParseTypeDescription::Int32(), order_basepage->column_basepages[O_W_ID_CID])});
    order_column_spec.insert({3, CSVColumnSpec(ParseTypeDescription::Int32(), order_basepage->column_basepages[O_C_ID_CID])});
    order_column_spec.insert({4, CSVColumnSpec(ParseTypeDescription::DateTime(), order_basepage->column_basepages[O_ENTRY_D_CID])});
    order_column_spec.insert({5, CSVColumnSpec(ParseTypeDescription::Int32(), order_basepage->column_basepages[O_CARRIER_ID_CID])});
    order_column_spec.insert({6, CSVColumnSpec(ParseTypeDescription::Int32(), order_basepage->column_basepages[O_OL_CNT_CID])});
    order_column_spec.insert({7, CSVColumnSpec(ParseTypeDescription::Int32(), order_basepage->column_basepages[O_ALL_LOCAL_CID])});
    pipelines.push_back(std::make_unique<CSVImportPipeline>(5, std::ref(db), joinPath(path, std::string("ORDER.tbl")), CSV_SEP, order_column_spec, 8, order_basepage->visibility_basepage));
    pipelines.back()->addDependency(pipelines.size() - 2);

    // ORDERLINE
    std::unordered_map<size_t, CSVColumnSpec> orderline_column_spec;
    ExclusiveGuard<TableBasepage> orderline_basepage(db.vmcache, db.getTableBasepageId("ORDERLINE", context.getWorkerId()), context.getWorkerId());
    orderline_column_spec.insert({0, CSVColumnSpec(ParseTypeDescription::Int32(), orderline_basepage->column_basepages[OL_O_ID_CID])});
    orderline_column_spec.insert({1, CSVColumnSpec(ParseTypeDescription::Int32(), orderline_basepage->column_basepages[OL_D_ID_CID])});
    orderline_column_spec.insert({2, CSVColumnSpec(ParseTypeDescription::Int32(), orderline_basepage->column_basepages[OL_W_ID_CID])});
    orderline_column_spec.insert({3, CSVColumnSpec(ParseTypeDescription::Int32(), orderline_basepage->column_basepages[OL_NUMBER_CID])});
    orderline_column_spec.insert({4, CSVColumnSpec(ParseTypeDescription::Int32(), orderline_basepage->column_basepages[OL_I_ID_CID])});
    orderline_column_spec.insert({5, CSVColumnSpec(ParseTypeDescription::Int32(), orderline_basepage->column_basepages[OL_SUPPLY_W_ID_CID])});
    orderline_column_spec.insert({6, CSVColumnSpec(ParseTypeDescription::DateTime(), orderline_basepage->column_basepages[OL_DELIVERY_D_CID])});
    orderline_column_spec.insert({7, CSVColumnSpec(ParseTypeDescription::Int32(), orderline_basepage->column_basepages[OL_QUANTITY_CID])});
    orderline_column_spec.insert({8, CSVColumnSpec(ParseTypeDescription::Decimal(2), orderline_basepage->column_basepages[OL_AMOUNT_CID])});
    orderline_column_spec.insert({9, CSVColumnSpec(ParseTypeDescription::Char(24), orderline_basepage->column_basepages[OL_DIST_INFO_CID])});
    pipelines.push_back(std::make_unique<CSVImportPipeline>(6, std::ref(db), joinPath(path, std::string("ORDERLINE.tbl")), CSV_SEP, orderline_column_spec, 10, orderline_basepage->visibility_basepage));
    pipelines.back()->addDependency(pipelines.size() - 2);

    // ITEM
    std::unordered_map<size_t, CSVColumnSpec> item_column_spec;
    ExclusiveGuard<TableBasepage> item_basepage(db.vmcache, db.getTableBasepageId("ITEM", context.getWorkerId()), context.getWorkerId());
    item_column_spec.insert({0, CSVColumnSpec(ParseTypeDescription::Int32(), item_basepage->column_basepages[I_ID_CID])});
    item_column_spec.insert({1, CSVColumnSpec(ParseTypeDescription::Int32(), item_basepage->column_basepages[I_IM_ID_CID])});
    item_column_spec.insert({2, CSVColumnSpec(ParseTypeDescription::Char(24), item_basepage->column_basepages[I_NAME_CID])});
    item_column_spec.insert({3, CSVColumnSpec(ParseTypeDescription::Decimal(2), item_basepage->column_basepages[I_PRICE_CID])});
    item_column_spec.insert({4, CSVColumnSpec(ParseTypeDescription::Char(50), item_basepage->column_basepages[I_DATA_CID])});
    pipelines.push_back(std::make_unique<CSVImportPipeline>(7, std::ref(db), joinPath(path, std::string("ITEM.tbl")), CSV_SEP, item_column_spec, 5, item_basepage->visibility_basepage));
    pipelines.back()->addDependency(pipelines.size() - 2);

    // STOCK
    std::unordered_map<size_t, CSVColumnSpec> stock_column_spec;
    ExclusiveGuard<TableBasepage> stock_basepage(db.vmcache, db.getTableBasepageId("STOCK", context.getWorkerId()), context.getWorkerId());
    stock_column_spec.insert({0, CSVColumnSpec(ParseTypeDescription::Int32(), stock_basepage->column_basepages[S_I_ID_CID])});
    stock_column_spec.insert({1, CSVColumnSpec(ParseTypeDescription::Int32(), stock_basepage->column_basepages[S_W_ID_CID])});
    stock_column_spec.insert({2, CSVColumnSpec(ParseTypeDescription::Int32(), stock_basepage->column_basepages[S_QUANTITY_CID])});
    stock_column_spec.insert({3, CSVColumnSpec(ParseTypeDescription::Char(24), stock_basepage->column_basepages[S_DIST_01_CID])});
    stock_column_spec.insert({4, CSVColumnSpec(ParseTypeDescription::Char(24), stock_basepage->column_basepages[S_DIST_02_CID])});
    stock_column_spec.insert({5, CSVColumnSpec(ParseTypeDescription::Char(24), stock_basepage->column_basepages[S_DIST_03_CID])});
    stock_column_spec.insert({6, CSVColumnSpec(ParseTypeDescription::Char(24), stock_basepage->column_basepages[S_DIST_04_CID])});
    stock_column_spec.insert({7, CSVColumnSpec(ParseTypeDescription::Char(24), stock_basepage->column_basepages[S_DIST_05_CID])});
    stock_column_spec.insert({8, CSVColumnSpec(ParseTypeDescription::Char(24), stock_basepage->column_basepages[S_DIST_06_CID])});
    stock_column_spec.insert({9, CSVColumnSpec(ParseTypeDescription::Char(24), stock_basepage->column_basepages[S_DIST_07_CID])});
    stock_column_spec.insert({10, CSVColumnSpec(ParseTypeDescription::Char(24), stock_basepage->column_basepages[S_DIST_08_CID])});
    stock_column_spec.insert({11, CSVColumnSpec(ParseTypeDescription::Char(24), stock_basepage->column_basepages[S_DIST_09_CID])});
    stock_column_spec.insert({12, CSVColumnSpec(ParseTypeDescription::Char(24), stock_basepage->column_basepages[S_DIST_10_CID])});
    stock_column_spec.insert({13, CSVColumnSpec(ParseTypeDescription::Int32(), stock_basepage->column_basepages[S_YTD_CID])});
    stock_column_spec.insert({14, CSVColumnSpec(ParseTypeDescription::Int32(), stock_basepage->column_basepages[S_ORDER_CNT_CID])});
    stock_column_spec.insert({15, CSVColumnSpec(ParseTypeDescription::Int32(), stock_basepage->column_basepages[S_REMOTE_CNT_CID])});
    stock_column_spec.insert({16, CSVColumnSpec(ParseTypeDescription::Char(50), stock_basepage->column_basepages[S_DATA_CID])});
    stock_column_spec.insert({17, CSVColumnSpec(ParseTypeDescription::Int32(), stock_basepage->column_basepages[S_SU_SUPPKEY_CID])});
    pipelines.push_back(std::make_unique<CSVImportPipeline>(8, std::ref(db), joinPath(path, std::string("STOCK.tbl")), CSV_SEP, stock_column_spec, 18, stock_basepage->visibility_basepage));
    pipelines.back()->addDependency(pipelines.size() - 2);

    // NATION
    std::unordered_map<size_t, CSVColumnSpec> nation_column_spec;
    ExclusiveGuard<TableBasepage> nation_basepage(db.vmcache, db.getTableBasepageId("NATION", context.getWorkerId()), context.getWorkerId());
    nation_column_spec.insert({0, CSVColumnSpec(ParseTypeDescription::Int32(), nation_basepage->column_basepages[N_NATIONKEY_CID])});
    nation_column_spec.insert({1, CSVColumnSpec(ParseTypeDescription::Char(25), nation_basepage->column_basepages[N_NAME_CID])});
    nation_column_spec.insert({2, CSVColumnSpec(ParseTypeDescription::Int32(), nation_basepage->column_basepages[N_REGIONKEY_CID])});
    nation_column_spec.insert({3, CSVColumnSpec(ParseTypeDescription::Char(152), nation_basepage->column_basepages[N_COMMENT_CID])});
    pipelines.push_back(std::make_unique<CSVImportPipeline>(9, std::ref(db), joinPath(path, std::string("NATION.tbl")), CSV_SEP, nation_column_spec, 4, nation_basepage->visibility_basepage));
    pipelines.back()->addDependency(pipelines.size() - 2);

    // SUPPLIER
    std::unordered_map<size_t, CSVColumnSpec> supplier_column_spec;
    ExclusiveGuard<TableBasepage> supplier_basepage(db.vmcache, db.getTableBasepageId("SUPPLIER", context.getWorkerId()), context.getWorkerId());
    supplier_column_spec.insert({0, CSVColumnSpec(ParseTypeDescription::Int32(), supplier_basepage->column_basepages[SU_SUPPKEY_CID])});
    supplier_column_spec.insert({1, CSVColumnSpec(ParseTypeDescription::Char(25), supplier_basepage->column_basepages[SU_NAME_CID])});
    supplier_column_spec.insert({2, CSVColumnSpec(ParseTypeDescription::Char(40), supplier_basepage->column_basepages[SU_ADDRESS_CID])});
    supplier_column_spec.insert({3, CSVColumnSpec(ParseTypeDescription::Int32(), supplier_basepage->column_basepages[SU_NATIONKEY_CID])});
    supplier_column_spec.insert({4, CSVColumnSpec(ParseTypeDescription::Char(15), supplier_basepage->column_basepages[SU_PHONE_CID])});
    supplier_column_spec.insert({5, CSVColumnSpec(ParseTypeDescription::Decimal(2), supplier_basepage->column_basepages[SU_ACCTBAL_CID])});
    supplier_column_spec.insert({6, CSVColumnSpec(ParseTypeDescription::Char(101), supplier_basepage->column_basepages[SU_COMMENT_CID])});
    pipelines.push_back(std::make_unique<CSVImportPipeline>(10, std::ref(db), joinPath(path, std::string("SUPPLIER.tbl")), CSV_SEP, supplier_column_spec, 7, supplier_basepage->visibility_basepage));
    pipelines.back()->addDependency(pipelines.size() - 2);

    // REGION
    std::unordered_map<size_t, CSVColumnSpec> region_column_spec;
    ExclusiveGuard<TableBasepage> region_basepage(db.vmcache, db.getTableBasepageId("REGION", context.getWorkerId()), context.getWorkerId());
    region_column_spec.insert({0, CSVColumnSpec(ParseTypeDescription::Int32(), region_basepage->column_basepages[R_REGIONKEY_CID])});
    region_column_spec.insert({1, CSVColumnSpec(ParseTypeDescription::Char(55), region_basepage->column_basepages[R_NAME_CID])});
    region_column_spec.insert({2, CSVColumnSpec(ParseTypeDescription::Char(152), region_basepage->column_basepages[R_COMMENT_CID])});
    pipelines.push_back(std::make_unique<CSVImportPipeline>(11, std::ref(db), joinPath(path, std::string("REGION.tbl")), CSV_SEP, region_column_spec, 3, region_basepage->visibility_basepage));
    pipelines.back()->addDependency(pipelines.size() - 2);

    QEP qep(std::move(pipelines));
    qep.begin(context);
    qep.waitForExecution(context, db.vmcache);
    const size_t num_warehouses = BTree<RowId, bool>(db.vmcache, warehouse_basepage->visibility_basepage, context.getWorkerId()).getCardinality();
    if (num_warehouses == 0)
        throw std::runtime_error("The import data did not contain any warehouses");
    const size_t district_cardinality = BTree<RowId, bool>(db.vmcache, district_basepage->visibility_basepage, context.getWorkerId()).getCardinality();
    if (district_cardinality != 10 * num_warehouses)
        throw UnexpectedCardinalityError("DISTRICT", district_cardinality, 10 * num_warehouses);
    const size_t customer_cardinality = BTree<RowId, bool>(db.vmcache, customer_basepage->visibility_basepage, context.getWorkerId()).getCardinality();
    if (customer_cardinality != 30000 * num_warehouses)
        throw UnexpectedCardinalityError("CUSTOMER", customer_cardinality, 30000 * num_warehouses);
    const size_t history_cardinality = BTree<RowId, bool>(db.vmcache, history_basepage->visibility_basepage, context.getWorkerId()).getCardinality();
    if (history_cardinality != 30000 * num_warehouses)
        throw UnexpectedCardinalityError("HISTORY", history_cardinality, 30000 * num_warehouses);
    const size_t neworder_cardinality = BTree<RowId, bool>(db.vmcache, neworder_basepage->visibility_basepage, context.getWorkerId()).getCardinality();
    if (neworder_cardinality != 9000 * num_warehouses)
        throw UnexpectedCardinalityError("NEWORDER", neworder_cardinality, 9000 * num_warehouses);
    const size_t order_cardinality = BTree<RowId, bool>(db.vmcache, order_basepage->visibility_basepage, context.getWorkerId()).getCardinality();
    if (order_cardinality != 30000 * num_warehouses)
        throw UnexpectedCardinalityError("ORDER", order_cardinality, 30000 * num_warehouses);
    const size_t orderline_cardinality = BTree<RowId, bool>(db.vmcache, orderline_basepage->visibility_basepage, context.getWorkerId()).getCardinality();
    if (orderline_cardinality != 300000 * num_warehouses)
        throw UnexpectedCardinalityError("ORDERLINE", orderline_cardinality, 300000 * num_warehouses);
    const size_t item_cardinality = BTree<RowId, bool>(db.vmcache, item_basepage->visibility_basepage, context.getWorkerId()).getCardinality();
    if (item_cardinality != 100000)
        throw UnexpectedCardinalityError("ITEM", item_cardinality, 100000);
    const size_t stock_cardinality = BTree<RowId, bool>(db.vmcache, stock_basepage->visibility_basepage, context.getWorkerId()).getCardinality();
    if (stock_cardinality != 100000 * num_warehouses)
        throw UnexpectedCardinalityError("STOCK", stock_cardinality, 100000 * num_warehouses);
    const size_t nation_cardinality = BTree<RowId, bool>(db.vmcache, nation_basepage->visibility_basepage, context.getWorkerId()).getCardinality();
    if (nation_cardinality != 62)
        throw UnexpectedCardinalityError("NATION", nation_cardinality, 62);
    const size_t supplier_cardinality = BTree<RowId, bool>(db.vmcache, supplier_basepage->visibility_basepage, context.getWorkerId()).getCardinality();
    if (supplier_cardinality != 10000)
        throw UnexpectedCardinalityError("SUPPLIER", supplier_cardinality, 10000);
    const size_t region_cardinality = BTree<RowId, bool>(db.vmcache, region_basepage->visibility_basepage, context.getWorkerId()).getCardinality();
    if (region_cardinality != 5)
        throw UnexpectedCardinalityError("REGION", region_cardinality, 5);
}

template <size_t n_keys>
bool validateIndexCardinality(DB& db, const std::string& table_name, size_t expected_cardinality, const uint32_t worker_id) {
    PageId basepage_id = db.getTableBasepageId(table_name, worker_id);
    SharedGuard<TableBasepage> basepage(db.vmcache, basepage_id, worker_id);
    size_t cardinality = BTree<CompositeKey<n_keys>, PageId>(db.vmcache, basepage->primary_key_index_basepage, worker_id).getCardinality();
    if (cardinality != expected_cardinality) {
        std::cerr << table_name << " primary key index has cardinality " << cardinality << ", expected " << expected_cardinality << std::endl;;
        return false;
    }
    return true;
}

bool validateDatabase(DB& db, bool full) {
    bool valid = true;
    const uint32_t worker_id = 0;

    PageId warehouse_basepage_id = db.getTableBasepageId("WAREHOUSE", worker_id);
    PageId warehouse_visibility_basepage = SharedGuard<TableBasepage>(db.vmcache, warehouse_basepage_id, worker_id)->visibility_basepage;
    const uint32_t num_warehouses = BTree<RowId, bool>(db.vmcache, warehouse_visibility_basepage, worker_id).getCardinality();
    if (num_warehouses == 0) {
        std::cerr << "The database does not contain any warehouses" << std::endl;
        return false;
    }

    valid &= validateIndexCardinality<1>(db, "WAREHOUSE", num_warehouses, worker_id);
    valid &= validateIndexCardinality<2>(db, "DISTRICT", 10 * num_warehouses, worker_id);
    if (full) {
        valid &= validateIndexCardinality<3>(db, "CUSTOMER", 30000 * num_warehouses, worker_id);
        valid &= validateIndexCardinality<3>(db, "NEWORDER", 9000 * num_warehouses, worker_id);
        valid &= validateIndexCardinality<3>(db, "ORDER", 30000 * num_warehouses, worker_id);
        valid &= validateIndexCardinality<4>(db, "ORDERLINE", 300000 * num_warehouses, worker_id);
        valid &= validateIndexCardinality<1>(db, "ITEM", 100000, worker_id);
        valid &= validateIndexCardinality<2>(db, "STOCK", 100000 * num_warehouses, worker_id);
    }
    valid &= validateIndexCardinality<1>(db, "NATION", 62, worker_id);
    valid &= validateIndexCardinality<1>(db, "SUPPLIER", 10000, worker_id);
    valid &= validateIndexCardinality<1>(db, "REGION", 5, worker_id);

    return valid;
}

} // namespace tpcch