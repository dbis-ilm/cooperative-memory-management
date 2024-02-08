#include "transactions.hpp"

#include <optional>

#include "execution/customer_select_index_scan.hpp"
#include "execution/os_order_select_index_scan.hpp"
#include "execution/sl_stock_select_index_scan.hpp"
#include "prototype/execution/qep.hpp"
#include "prototype/execution/sort.hpp"
#include "prototype/execution/temporary_column.hpp"
#include "tpcch.hpp"

namespace tpcch {

const auto COUNT = NamedColumn(std::string("COUNT(*)"), std::make_shared<UnencodedTemporaryColumn<Integer>>());

std::shared_ptr<DefaultBreaker> executeSynchronouslyWithDefaultBreaker(DB& db, std::vector<std::unique_ptr<ExecutablePipeline>>&& pipelines, const ExecutionContext context) {
    pipelines.back()->addDefaultBreaker(context);
    auto qep = std::make_shared<QEP>(std::move(pipelines));
    qep->begin(context);
    qep->waitForExecution(context, db.vmcache, false);
    auto result = std::dynamic_pointer_cast<DefaultBreaker>(qep->getResult());
    assert(result != nullptr);
    return result;
}

uint64_t runNOWarehouseSelect(DB& db, Identifier w_id, const ExecutionContext context) {
    // select W_TAX from WAREHOUSE where W_ID=?
    std::vector<std::unique_ptr<ExecutablePipeline>> pipelines;
    pipelines.push_back(std::make_unique<ExecutablePipeline>(
        0, db, "WAREHOUSE", CompositeKey<1> { w_id },
        std::vector<NamedColumn>({ W_TAX }),
        context)
    );
    auto result = executeSynchronouslyWithDefaultBreaker(db, std::move(pipelines), context);
    std::vector<std::shared_ptr<Batch>> batches;
    result->consumeBatches(batches, context.getWorkerId());
    return *reinterpret_cast<uint64_t*>(batches.front()->getRow(0));
}

std::pair<uint64_t, Identifier> runNODistrictUpdate(DB& db, Identifier w_id, Identifier d_id, const ExecutionContext context) {
    // update DISTRICT set D_NEXT_O_ID=D_NEXT_O_ID+1 where D_W_ID=? and D_ID=?
    std::vector<std::unique_ptr<ExecutablePipeline>> pipelines;
    pipelines.push_back(std::make_unique<ExecutablePipeline>(
        0, db, "DISTRICT", CompositeKey<2> { d_id, w_id },
        std::vector<NamedColumn>({ D_TAX, D_NEXT_O_ID }),
        std::vector<std::function<void(void*)>>({
            [](void*) { }, // do not update D_TAX, we just want the current value
            [](void* data) { *reinterpret_cast<Identifier*>(data) += 1; }
        }),
        context)
    );
    auto result = executeSynchronouslyWithDefaultBreaker(db, std::move(pipelines), context);
    std::vector<std::shared_ptr<Batch>> batches;
    BatchDescription desc;
    result->consumeBatches(batches, context.getWorkerId());
    result->consumeBatchDescription(desc);
    const char* row_ptr = reinterpret_cast<const char*>(batches.front()->getRow(0));
    // note: we subtract one from D_NEXT_O_ID here to get the value that the attribute had before the update
    return std::make_pair(*reinterpret_cast<const uint64_t*>(row_ptr + desc.find("D_TAX").offset), (*reinterpret_cast<const Identifier*>(row_ptr + desc.find("D_NEXT_O_ID").offset)) - 1);
}

void runNOCustomerSelect(DB& db, Identifier w_id, Identifier d_id, Identifier c_id, uint64_t& c_discount, std::string& c_last, std::string& c_credit, const ExecutionContext context) {
    // select C_DISCOUNT,C_LAST,C_CREDIT from CUSTOMER where C_W_ID=? and C_D_ID=? and C_ID=?
    std::vector<std::unique_ptr<ExecutablePipeline>> pipelines;
    pipelines.push_back(std::make_unique<ExecutablePipeline>(
        0, db, "CUSTOMER", CompositeKey<3> { d_id, w_id, c_id },
        std::vector<NamedColumn>({ C_DISCOUNT, C_LAST, C_CREDIT }),
        context)
    );
    auto result = executeSynchronouslyWithDefaultBreaker(db, std::move(pipelines), context);
    std::vector<std::shared_ptr<Batch>> batches;
    result->consumeBatches(batches, context.getWorkerId());
    c_discount = *reinterpret_cast<uint64_t*>(batches.front()->getRow(0));
    c_last = std::string(reinterpret_cast<char*>(batches.front()->getRow(0)) + C_DISCOUNT.column->getValueTypeSize(), C_LAST.column->getValueTypeSize());
    c_credit = std::string(reinterpret_cast<char*>(batches.front()->getRow(0)) + C_DISCOUNT.column->getValueTypeSize() + C_LAST.column->getValueTypeSize(), C_CREDIT.column->getValueTypeSize());
}

void runNOOrderInsert(DB& db, Identifier o_d_id, Identifier o_w_id, Identifier o_id, Identifier o_c_id, DateTime o_entry_d, Integer o_ol_cnt, bool o_all_local, const ExecutionContext context) {
    // insert into \"ORDER\" values (?,?,?,?,?,NULL,?,?)
    SharedGuard<TableBasepage> table(db.vmcache, db.getTableBasepageId("ORDER", context.getWorkerId()), context.getWorkerId());
    auto insert_guard = BTree<RowId, bool>(db.vmcache, table->visibility_basepage, context.getWorkerId()).insertNext(true);
    assert(table->primary_key_index_basepage != INVALID_PAGE_ID);
    BTree<CompositeKey<3>, size_t> pkey(db.vmcache, table->primary_key_index_basepage, context.getWorkerId());
    pkey.insert(CompositeKey<3> { o_d_id, o_w_id, o_id }, insert_guard.key);
    BTree<CompositeKey<4>, size_t> wdc(db.vmcache, table->additional_index_basepage, context.getWorkerId());
    wdc.insert(CompositeKey<4> { o_d_id, o_w_id, o_c_id, o_id }, insert_guard.key);
    db.appendFixedSizeValue(insert_guard.key, table->column_basepages[O_D_ID_CID], &o_d_id, sizeof(Identifier), context.getWorkerId());
    db.appendFixedSizeValue(insert_guard.key, table->column_basepages[O_W_ID_CID], &o_w_id, sizeof(Identifier), context.getWorkerId());
    db.appendFixedSizeValue(insert_guard.key, table->column_basepages[O_ID_CID], &o_id, sizeof(Identifier), context.getWorkerId());
    db.appendFixedSizeValue(insert_guard.key, table->column_basepages[O_C_ID_CID], &o_c_id, sizeof(Identifier), context.getWorkerId());
    db.appendFixedSizeValue(insert_guard.key, table->column_basepages[O_ENTRY_D_CID], &o_entry_d, sizeof(DateTime), context.getWorkerId());
    Identifier null = 0;
    db.appendFixedSizeValue(insert_guard.key, table->column_basepages[O_CARRIER_ID_CID], &null, sizeof(Identifier), context.getWorkerId());
    db.appendFixedSizeValue(insert_guard.key, table->column_basepages[O_OL_CNT_CID], &o_ol_cnt, sizeof(Integer), context.getWorkerId());
    Integer all_local = o_all_local ? 1 : 0;
    db.appendFixedSizeValue(insert_guard.key, table->column_basepages[O_ALL_LOCAL_CID], &all_local, sizeof(Integer), context.getWorkerId());
}

void runNONewOrderInsert(DB& db, Identifier no_o_id, Identifier no_d_id, Identifier no_w_id, const ExecutionContext context) {
    // insert into NEWORDER values(?,?,?)
    SharedGuard<TableBasepage> table(db.vmcache, db.getTableBasepageId("NEWORDER", context.getWorkerId()), context.getWorkerId());
    auto insert_guard = BTree<RowId, bool>(db.vmcache, table->visibility_basepage, context.getWorkerId()).insertNext(true);
    assert(table->primary_key_index_basepage != INVALID_PAGE_ID);
    BTree<CompositeKey<3>, size_t> pkey(db.vmcache, table->primary_key_index_basepage, context.getWorkerId());
    pkey.insert(CompositeKey<3> { no_d_id, no_w_id, no_o_id }, insert_guard.key);
    db.appendFixedSizeValue(insert_guard.key, table->column_basepages[NO_D_ID_CID], &no_d_id, sizeof(Identifier), context.getWorkerId());
    db.appendFixedSizeValue(insert_guard.key, table->column_basepages[NO_W_ID_CID], &no_w_id, sizeof(Identifier), context.getWorkerId());
    db.appendFixedSizeValue(insert_guard.key, table->column_basepages[NO_O_ID_CID], &no_o_id, sizeof(Identifier), context.getWorkerId());
}

bool runNOItemSelect(DB& db, Identifier i_id, uint64_t& i_price, const ExecutionContext context) {
    // select I_PRICE,I_NAME,I_DATA from ITEM where I_ID=?
    std::vector<std::unique_ptr<ExecutablePipeline>> pipelines;
    pipelines.push_back(std::make_unique<ExecutablePipeline>(
        0, db, "ITEM", CompositeKey<1> { i_id },
        std::vector<NamedColumn>({ I_PRICE }),
        context)
    );
    auto result = executeSynchronouslyWithDefaultBreaker(db, std::move(pipelines), context);
    std::vector<std::shared_ptr<Batch>> batches;
    result->consumeBatches(batches, context.getWorkerId());
    if (batches.empty() || batches.front()->getValidRowCount() == 0)
        return false;
    i_price = *reinterpret_cast<uint64_t*>(batches.front()->getRow(0));
    return true;
}

void runNOStockSelect(DB& db, Identifier d_id, Identifier i_id, Identifier w_id, int32_t& s_quantity, std::string& s_dist, const ExecutionContext context) {
    // select S_QUANTITY,S_DIST_<d_id>,S_DATA from STOCK where S_I_ID=? and S_W_ID=?
    std::vector<std::unique_ptr<ExecutablePipeline>> pipelines;
    const auto s_dist_col = NamedColumn("S_DIST", std::make_shared<UnencodedTableColumn<Char<24>>>(S_DIST_01_CID + d_id - 1));
    pipelines.push_back(std::make_unique<ExecutablePipeline>(
        0, db, "STOCK", CompositeKey<2> { i_id, w_id },
        std::vector<NamedColumn>({ S_QUANTITY, s_dist_col }),
        context)
    );
    auto result = executeSynchronouslyWithDefaultBreaker(db, std::move(pipelines), context);
    std::vector<std::shared_ptr<Batch>> batches;
    result->consumeBatches(batches, context.getWorkerId());
    s_quantity = *reinterpret_cast<int32_t*>(batches.front()->getRow(0));
    s_dist = std::string(reinterpret_cast<char*>(batches.front()->getRow(0)) + S_QUANTITY.column->getValueTypeSize(), s_dist_col.column->getValueTypeSize());
}

size_t runNOStockUpdate(DB& db, Identifier i_id, Identifier w_id, bool is_remote, int32_t ol_quantity, int32_t s_quantity, const ExecutionContext context) {
    // if 'is_remote'
    //  update STOCK set S_YTD=S_YTD+?, S_ORDER_CNT=S_ORDER_CNT+1, S_QUANTITY=? where S_I_ID=? and S_W_ID=?
    // else
    //  update STOCK set S_YTD=S_YTD+?, S_ORDER_CNT=S_ORDER_CNT+1, S_QUANTITY=?, S_REMOTE_CNT=S_REMOTE_CNT+1 where S_I_ID=? and S_W_ID=?
    std::vector<std::unique_ptr<ExecutablePipeline>> pipelines;
    auto update_columns = std::vector<NamedColumn>({ S_YTD, S_ORDER_CNT, S_QUANTITY });
    auto updates = std::vector<std::function<void(void*)>>({
        [ol_quantity](void* data) { *reinterpret_cast<Integer*>(data) += ol_quantity; },// S_YTD=S_YTD+?
        [](void* data) { *reinterpret_cast<Integer*>(data) += 1; },                     // S_ORDER_CNT=S_ORDER_CNT+1
        [s_quantity](void* data) { *reinterpret_cast<Integer*>(data) = s_quantity; }    // S_QUANTITY=?
    });
    if (is_remote) {
        update_columns.push_back(S_REMOTE_CNT);
        updates.push_back([](void* data) { *reinterpret_cast<Integer*>(data) += 1; });  // S_REMOTE_CNT=S_REMOTE_CNT+1
    }
    pipelines.push_back(std::make_unique<ExecutablePipeline>(
        0, db, "STOCK", CompositeKey<2> { i_id, w_id },
        std::move(update_columns),
        std::move(updates),
        context)
    );
    return executeSynchronouslyWithDefaultBreaker(db, std::move(pipelines), context)->getValidRowCount();
}

void runNOOrderlineInsert(DB& db, Identifier ol_d_id, Identifier ol_w_id, Identifier ol_o_id, Identifier ol_number, Identifier ol_i_id, Identifier ol_supply_w_id, Integer ol_quantity, uint64_t ol_amount, std::string& ol_dist_info, const ExecutionContext context) {
    // insert into ORDERLINE values (?,?,?,?,?,?,NULL,?,?,?)
    SharedGuard<TableBasepage> table(db.vmcache, db.getTableBasepageId("ORDERLINE", context.getWorkerId()), context.getWorkerId());
    auto insert_guard = BTree<RowId, bool>(db.vmcache, table->visibility_basepage, context.getWorkerId()).insertNext(true);
    assert(table->primary_key_index_basepage != INVALID_PAGE_ID);
    BTree<CompositeKey<4>, size_t> pkey(db.vmcache, table->primary_key_index_basepage, context.getWorkerId());
    pkey.insert(CompositeKey<4> { ol_d_id, ol_w_id, ol_o_id, ol_number }, insert_guard.key);
    db.appendFixedSizeValue(insert_guard.key, table->column_basepages[OL_D_ID_CID], &ol_d_id, sizeof(Identifier), context.getWorkerId());
    db.appendFixedSizeValue(insert_guard.key, table->column_basepages[OL_W_ID_CID], &ol_w_id, sizeof(Identifier), context.getWorkerId());
    db.appendFixedSizeValue(insert_guard.key, table->column_basepages[OL_O_ID_CID], &ol_o_id, sizeof(Identifier), context.getWorkerId());
    db.appendFixedSizeValue(insert_guard.key, table->column_basepages[OL_NUMBER_CID], &ol_number, sizeof(Identifier), context.getWorkerId());
    db.appendFixedSizeValue(insert_guard.key, table->column_basepages[OL_I_ID_CID], &ol_i_id, sizeof(Identifier), context.getWorkerId());
    db.appendFixedSizeValue(insert_guard.key, table->column_basepages[OL_SUPPLY_W_ID_CID], &ol_supply_w_id, sizeof(Identifier), context.getWorkerId());
    uint64_t null = 0;
    db.appendFixedSizeValue(insert_guard.key, table->column_basepages[OL_DELIVERY_D_CID], &null, sizeof(DateTime), context.getWorkerId());
    db.appendFixedSizeValue(insert_guard.key, table->column_basepages[OL_QUANTITY_CID], &ol_quantity, sizeof(Integer), context.getWorkerId());
    db.appendFixedSizeValue(insert_guard.key, table->column_basepages[OL_AMOUNT_CID], &ol_amount, sizeof(Decimal<2>), context.getWorkerId());
    char dist_info[24] = {};
    memcpy(dist_info, ol_dist_info.c_str(), std::min(ol_dist_info.size(), 24ul));
    db.appendFixedSizeValue(insert_guard.key, table->column_basepages[OL_DIST_INFO_CID], dist_info, 24, context.getWorkerId());
}

bool runNewOrder(std::ostream& log, DB& db, Identifier w_id, Identifier d_id, Identifier c_id, const OrderLine* orderlines, uint32_t ol_cnt, bool all_local, DateTime o_entry_d, const ExecutionContext context) {
    // BEGIN TRANSACTION
    uint64_t w_tax = runNOWarehouseSelect(db, w_id, context);

    auto district_select_result = runNODistrictUpdate(db, w_id, d_id, context);
    uint64_t d_tax = district_select_result.first;
    Identifier d_next_oid = district_select_result.second;

    uint64_t c_discount;
    std::string c_last;
    std::string c_credit;
    runNOCustomerSelect(db, w_id, d_id, c_id, c_discount, c_last, c_credit, context);

    runNOOrderInsert(db, d_id, w_id, d_next_oid, c_id, o_entry_d, ol_cnt, all_local, context);

    runNONewOrderInsert(db, d_next_oid, d_id, w_id, context);

    uint64_t i_price;
    uint64_t total_amount = 0;
	int32_t s_quantity;
	std::string s_dist;
	for (size_t i = 0; i < ol_cnt; i++) {
        if (!runNOItemSelect(db, orderlines[i].ol_i_id, i_price, context)) {
            // expected ROLLBACK
            log << "NewOrder rolled back" << std::endl;
            return true;
        }

        runNOStockSelect(db, d_id, orderlines[i].ol_i_id, orderlines[i].ol_supply_w_id, s_quantity, s_dist, context);

		int32_t new_s_quantity = 0;
		if (orderlines[i].ol_quantity <= s_quantity - 10) {
			new_s_quantity = s_quantity - orderlines[i].ol_quantity;
        } else {
			new_s_quantity = s_quantity - orderlines[i].ol_quantity + 91;
        }
        runNOStockUpdate(db, orderlines[i].ol_i_id, orderlines[i].ol_supply_w_id, orderlines[i].ol_is_remote, orderlines[i].ol_quantity, new_s_quantity, context);

        uint64_t ol_amount = i_price * orderlines[i].ol_quantity;
        runNOOrderlineInsert(db, d_id, w_id, d_next_oid, i + 1, orderlines[i].ol_i_id, orderlines[i].ol_supply_w_id, orderlines[i].ol_quantity, ol_amount, s_dist, context);
        total_amount += ol_amount;
	}
    log << "[NewOrder] (" << o_entry_d << ") Tax: " << Decimal<2>(w_tax) << " + " << Decimal<2>(d_tax) << ", Total (" << ol_cnt << " order lines): " << Decimal<2>(total_amount);
    if (all_local)
        log << " (all local)";
    log << std::endl;
    return true;
}

std::string runPMWarehouseSelect(DB& db, Identifier w_id, const ExecutionContext context) {
    // select W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP from WAREHOUSE where W_ID=?
    std::vector<std::unique_ptr<ExecutablePipeline>> pipelines;
    pipelines.push_back(std::make_unique<ExecutablePipeline>(
        0, db, "WAREHOUSE", CompositeKey<1> { w_id },
        std::vector<NamedColumn>({ W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP }),
        context)
    );
    auto result = executeSynchronouslyWithDefaultBreaker(db, std::move(pipelines), context);
    std::vector<std::shared_ptr<Batch>> batches;
    result->consumeBatches(batches, context.getWorkerId());
    char buf[11] = {};
    memcpy(buf, batches.front()->getRow(0), 10);
    return std::string(buf);
}

void runPMWarehouseUpdate(DB& db, Identifier w_id, Decimal<2> h_amount, const ExecutionContext context) {
    // update WAREHOUSE set W_YTD = W_YTD + ? where W_ID=?
    std::vector<std::unique_ptr<ExecutablePipeline>> pipelines;
    pipelines.push_back(std::make_unique<ExecutablePipeline>(
        0, db, "WAREHOUSE", CompositeKey<1> { w_id },
        std::vector<NamedColumn>({ W_YTD }),
        std::vector<std::function<void(void*)>>({ [h_amount](void* data) { *reinterpret_cast<Decimal<2>*>(data) += h_amount; } }),
        context)
    );
    executeSynchronouslyWithDefaultBreaker(db, std::move(pipelines), context);
}

std::string runPMDistrictSelect(DB& db, Identifier w_id, Identifier d_id, const ExecutionContext context) {
    // select D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP from DISTRICT where W_ID=? and D_ID=?
    std::vector<std::unique_ptr<ExecutablePipeline>> pipelines;
    pipelines.push_back(std::make_unique<ExecutablePipeline>(
        0, db, "DISTRICT", CompositeKey<2> { d_id, w_id },
        std::vector<NamedColumn>({ D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP }),
        context)
    );
    auto result = executeSynchronouslyWithDefaultBreaker(db, std::move(pipelines), context);
    std::vector<std::shared_ptr<Batch>> batches;
    result->consumeBatches(batches, context.getWorkerId());
    char buf[11] = {};
    memcpy(buf, batches.front()->getRow(0), 10);
    return std::string(buf);
}

void runPMDistrictUpdate(DB& db, Identifier w_id, Identifier d_id, Decimal<2> h_amount, const ExecutionContext context) {
    // update DISTRICT set D_YTD = D_YTD + ? where W_ID=? and D_ID=?
    std::vector<std::unique_ptr<ExecutablePipeline>> pipelines;
    pipelines.push_back(std::make_unique<ExecutablePipeline>(
        0, db, "DISTRICT", CompositeKey<2> { d_id, w_id },
        std::vector<NamedColumn>({ D_YTD }),
        std::vector<std::function<void(void*)>>({ [h_amount](void* data) { *reinterpret_cast<Decimal<2>*>(data) += h_amount; } }),
        context)
    );
    auto result = executeSynchronouslyWithDefaultBreaker(db, std::move(pipelines), context);
}

Identifier runPMCustomerSelect1(DB& db, Identifier w_id, Identifier d_id, std::string c_last, const ExecutionContext context) {
    // select C_ID from CUSTOMER where C_LAST=? and C_D_ID=? and C_W_ID=? order by C_FIRST asc
    //  return the C_ID for the middle customer from the result set (n/2 rounded *up* to the next integer)
    std::vector<std::unique_ptr<ExecutablePipeline>> pipelines;
    pipelines.push_back(std::make_unique<ExecutablePipeline>(pipelines.size()));
    pipelines.back()->addOperator(std::make_shared<CustomerSelectIndexScanOperator>(
        db, c_last, C_LAST,
        CompositeKey<3> { d_id, w_id, std::numeric_limits<Identifier>::min() },
        CompositeKey<3> { d_id, w_id, std::numeric_limits<Identifier>::max() },
        std::vector<NamedColumn>({ C_ID, C_FIRST }),
        context));
    pipelines.back()->current_columns.addColumn("C_ID", C_ID.column);
    pipelines.back()->current_columns.addColumn("C_FIRST", C_FIRST.column);
    // sort ascending by C_FIRST
    const size_t c_first_offset = pipelines.back()->current_columns.find("C_FIRST").offset;
    pipelines.back()->addSortBreaker([c_first_offset](const Row& a, const Row& b) -> int {
        return memcmp(reinterpret_cast<const char*>(a.data) + c_first_offset, reinterpret_cast<const char*>(b.data) + c_first_offset, 16);
    }, context.getWorkerCount());
    pipelines.push_back(std::make_unique<ExecutablePipeline>(pipelines.size()));
    pipelines.back()->addSort(db.vmcache, *pipelines[0].get());
    auto result = executeSynchronouslyWithDefaultBreaker(db, std::move(pipelines), context);
    // get "middle" C_ID from the result set
    std::vector<std::shared_ptr<Batch>> batches;
    size_t n = result->getValidRowCount();
    assert(n != 0);
    result->consumeBatches(batches, context.getWorkerId());
    size_t target = (n + 1) / 2 - 1;
    size_t i = 0;
    size_t batch_i = 0;
    while (true) {
        if (target - i < batches[batch_i]->getValidRowCount()) {
            return *reinterpret_cast<Identifier*>(batches[batch_i]->getRow(target - i));
        }
        i += batches[batch_i]->getValidRowCount();
        batch_i++;
    }
}

std::string runPMCustomerSelect2(DB& db, Identifier w_id, Identifier d_id, Identifier c_id, const ExecutionContext context) {
    // select C_ID, C_FIRST, C_MIDDLE, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE from CUSTOMER where C_D_ID=? and C_W_ID=? and C_ID=?
    //  return C_CREDT
    std::vector<std::unique_ptr<ExecutablePipeline>> pipelines;
    pipelines.push_back(std::make_unique<ExecutablePipeline>(
        0, db, "CUSTOMER", CompositeKey<3> { d_id, w_id, c_id },
        std::vector<NamedColumn>({ C_ID, C_FIRST, C_MIDDLE, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE }),
        context)
    );
    auto result = executeSynchronouslyWithDefaultBreaker(db, std::move(pipelines), context);
    std::vector<std::shared_ptr<Batch>> batches;
    BatchDescription desc;
    result->consumeBatches(batches, context.getWorkerId());
    result->consumeBatchDescription(desc);
    auto c_credit_desc = desc.find("C_CREDIT");
    const char* c_credit = reinterpret_cast<char*>(batches.front()->getRow(0)) + c_credit_desc.offset;
    return std::string(c_credit, c_credit[c_credit_desc.column->getValueTypeSize() - 1] != 0 ? c_credit_desc.column->getValueTypeSize() : strlen(c_credit));
}

void runPMCustomerUpdate(DB& db, Identifier w_id, Identifier d_id, Identifier c_id, Decimal<2> h_amount, std::optional<std::string> c_data_prefix, const ExecutionContext context) {
    std::vector<std::unique_ptr<ExecutablePipeline>> pipelines;
    auto update_columns = std::vector<NamedColumn>({ C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT });
    auto updates = std::vector<std::function<void(void*)>>({
        [h_amount](void* data) { *reinterpret_cast<Decimal<2>*>(data) -= h_amount; },   // C_BALANCE = C_BALANCE - h_amount
        [h_amount](void* data) { *reinterpret_cast<Decimal<2>*>(data) += h_amount; },   // C_YTD_PAYMENT = C_YTD_PAYMENT + h_amount
        [](void* data) { *reinterpret_cast<Integer*>(data) += 1; }                      // C_PAYMENT_CNT = C_PAYMENT_CNT + 1
    });
    if (c_data_prefix) {
        update_columns.push_back(C_DATA);
        std::string c_data_prefix_str = c_data_prefix.value();
        updates.push_back([c_data_prefix_str](void* data) {
            size_t copy_size = std::min(c_data_prefix_str.size(), 500ul);
            size_t rem = 500 - copy_size;
            if (rem > 0)
                memmove(reinterpret_cast<char*>(data) + copy_size, data, rem);
            memcpy(data, c_data_prefix_str.c_str(), copy_size);
        });
    }
    pipelines.push_back(std::make_unique<ExecutablePipeline>(
        0, db, "CUSTOMER", CompositeKey<3> { d_id, w_id, c_id },
        std::move(update_columns),
        std::move(updates),
        context)
    );
    executeSynchronouslyWithDefaultBreaker(db, std::move(pipelines), context);
}

void runPMHistoryInsert(DB& db, Identifier c_id, Identifier c_d_id, Identifier c_w_id, Identifier d_id, Identifier w_id, DateTime h_date, Decimal<2> h_amount, const std::string& h_data, const ExecutionContext context) {
    // insert into HISTORY values (?,?,?,?,?,?,?,?)
    SharedGuard<TableBasepage> table(db.vmcache, db.getTableBasepageId("HISTORY", context.getWorkerId()), context.getWorkerId());
    auto insert_guard = BTree<RowId, bool>(db.vmcache, table->visibility_basepage, context.getWorkerId()).insertNext(true);
    // note: HISTORY does not have a primary key, so no index insert needed here
    db.appendFixedSizeValue(insert_guard.key, table->column_basepages[H_C_ID_CID], &c_id, sizeof(Identifier), context.getWorkerId());
    db.appendFixedSizeValue(insert_guard.key, table->column_basepages[H_C_D_ID_CID], &c_d_id, sizeof(Identifier), context.getWorkerId());
    db.appendFixedSizeValue(insert_guard.key, table->column_basepages[H_C_W_ID_CID], &c_w_id, sizeof(Identifier), context.getWorkerId());
    db.appendFixedSizeValue(insert_guard.key, table->column_basepages[H_D_ID_CID], &d_id, sizeof(Identifier), context.getWorkerId());
    db.appendFixedSizeValue(insert_guard.key, table->column_basepages[H_W_ID_CID], &w_id, sizeof(Identifier), context.getWorkerId());
    db.appendFixedSizeValue(insert_guard.key, table->column_basepages[H_DATE_CID], &h_date, sizeof(DateTime), context.getWorkerId());
    db.appendFixedSizeValue(insert_guard.key, table->column_basepages[H_AMOUNT_CID], &h_amount, sizeof(Decimal<2>), context.getWorkerId());
    char data[24] = {};
    memcpy(data, h_data.c_str(), std::min(h_data.size(), 24ul));
    db.appendFixedSizeValue(insert_guard.key, table->column_basepages[H_DATA_CID], &data, 24, context.getWorkerId());
}

bool runPayment(std::ostream& log, DB& db, Identifier w_id, Identifier d_id, Identifier c_w_id, Identifier c_d_id, bool customer_based_on_last_name, Identifier c_id, const std::string& c_last, Decimal<2> h_amount, DateTime h_date, const ExecutionContext context) {
	// BEGIN TRANSACTION
    std::string w_name = runPMWarehouseSelect(db, w_id, context);
    runPMWarehouseUpdate(db, w_id, h_amount, context);
    std::string d_name = runPMDistrictSelect(db, w_id, d_id, context);
    runPMDistrictUpdate(db, w_id, d_id, h_amount, context);

    std::string c_credit;
	if (customer_based_on_last_name) {
        c_id = runPMCustomerSelect1(db, c_w_id, c_d_id, c_last, context);
	}
    c_credit = runPMCustomerSelect2(db, c_w_id, c_d_id, c_id, context);

	if (c_credit == "BC") {
        std::stringstream c_data_prefix;
        c_data_prefix << c_id << "," << c_d_id << "," << c_w_id << "," << d_id << ","  << w_id << "," << h_amount << ",";
        runPMCustomerUpdate(db, c_w_id, c_d_id, c_id, h_amount, std::make_optional(c_data_prefix.str()), context);
	} else {
        runPMCustomerUpdate(db, c_w_id, c_d_id, c_id, h_amount, std::nullopt, context);
    }

	std::string h_data = w_name + "    " + d_name;
    runPMHistoryInsert(db, c_id, c_d_id, c_w_id, d_id, w_id, h_date, h_amount, h_data, context);

    log << "[Payment] " << c_w_id << "," << c_d_id << "," << c_id << " (" << c_credit << ") " << h_data << std::endl;
    return true;
}

Identifier runOSCustomerSelect1(DB& db, Identifier w_id, Identifier d_id, const std::string& c_last, const ExecutionContext context) {
    // select C_ID, C_BALANCE, C_FIRST, C_MIDDLE, C_LAST from TPCCH.CUSTOMER where C_LAST=? and C_D_ID=? and C_W_ID=? order by C_FIRST asc
    std::vector<std::unique_ptr<ExecutablePipeline>> pipelines;
    pipelines.push_back(std::make_unique<ExecutablePipeline>(pipelines.size()));
    pipelines.back()->addOperator(std::make_shared<CustomerSelectIndexScanOperator>(
        db, c_last, C_LAST,
        CompositeKey<3> { d_id, w_id, std::numeric_limits<Identifier>::min() },
        CompositeKey<3> { d_id, w_id, std::numeric_limits<Identifier>::max() },
        std::vector<NamedColumn>({ C_ID, C_BALANCE, C_FIRST, C_MIDDLE, C_LAST }),
        context));
    pipelines.back()->current_columns.addColumn("C_ID", C_ID.column);
    pipelines.back()->current_columns.addColumn("C_BALANCE", C_BALANCE.column);
    pipelines.back()->current_columns.addColumn("C_FIRST", C_FIRST.column);
    pipelines.back()->current_columns.addColumn("C_MIDDLE", C_MIDDLE.column);
    pipelines.back()->current_columns.addColumn("C_LAST", C_LAST.column);
    // sort ascending by C_FIRST
    const size_t c_first_offset = pipelines.back()->current_columns.find("C_FIRST").offset;
    pipelines.back()->addSortBreaker([c_first_offset](const Row& a, const Row& b) -> int {
        return memcmp(reinterpret_cast<const char*>(a.data) + c_first_offset, reinterpret_cast<const char*>(b.data) + c_first_offset, 16);
    }, context.getWorkerCount());
    pipelines.push_back(std::make_unique<ExecutablePipeline>(pipelines.size()));
    pipelines.back()->addSort(db.vmcache, *pipelines[0].get());
    auto result = executeSynchronouslyWithDefaultBreaker(db, std::move(pipelines), context);
    std::vector<std::shared_ptr<Batch>> batches;
    size_t n = result->getValidRowCount();
    assert(n != 0);
    result->consumeBatches(batches, context.getWorkerId());
    size_t target = (n + 1) / 2 - 1;
    size_t i = 0;
    size_t batch_i = 0;
    while (true) {
        if (target - i < batches[batch_i]->getValidRowCount()) {
            return *reinterpret_cast<Identifier*>(batches[batch_i]->getRow(target - i));
        }
        i += batches[batch_i]->getValidRowCount();
        batch_i++;
    }
}

std::string runOSCustomerSelect2(DB& db, Identifier c_id, Identifier w_id, Identifier d_id, const ExecutionContext context) {
    // select C_BALANCE, C_FIRST, C_MIDDLE, C_LAST from TPCCH.CUSTOMER where C_ID=? and C_D_ID=? and C_W_ID=?
    std::vector<std::unique_ptr<ExecutablePipeline>> pipelines;
    pipelines.push_back(std::make_unique<ExecutablePipeline>(
        0, db, "CUSTOMER", CompositeKey<3> { d_id, w_id, c_id },
        std::vector<NamedColumn>({ C_BALANCE, C_FIRST, C_MIDDLE, C_LAST }),
        context)
    );
    auto result = executeSynchronouslyWithDefaultBreaker(db, std::move(pipelines), context);
    std::vector<std::shared_ptr<Batch>> batches;
    BatchDescription desc;
    result->consumeBatches(batches, context.getWorkerId());
    result->consumeBatchDescription(desc);
    auto c_last_desc = desc.find("C_LAST");
    const char* c_last = reinterpret_cast<char*>(batches.front()->getRow(0)) + c_last_desc.offset;
    return std::string(c_last, c_last[c_last_desc.column->getValueTypeSize() - 1] != 0 ? c_last_desc.column->getValueTypeSize() : strlen(c_last));
}

Identifier runOSOrderSelect(DB& db, Identifier w_id, Identifier d_id, Identifier c_id, const ExecutionContext context) {
    // select O_ID, O_ENTRY_D, O_CARRIER_ID from TPCCH.\"ORDER\" where O_W_ID=? and O_D_ID=? and O_C_ID=? and O_ID=(select max(O_ID) from TPCCH.\"ORDER\" where O_W_ID=? and O_D_ID=? and O_C_ID=?)
    std::vector<std::unique_ptr<ExecutablePipeline>> pipelines;
    pipelines.push_back(std::make_unique<ExecutablePipeline>(pipelines.size()));
    pipelines.back()->addOperator(std::make_shared<OSOrderSelectIndexScanOperator>(
        db,
        CompositeKey<4> { d_id, w_id, c_id, std::numeric_limits<Identifier>::min() },
        CompositeKey<4> { d_id, w_id, c_id, std::numeric_limits<Identifier>::max() },
        std::vector<NamedColumn>({ O_ID, O_ENTRY_D, O_CARRIER_ID }),
        context));
    pipelines.back()->current_columns.addColumn("O_ID", O_ID.column);
    pipelines.back()->current_columns.addColumn("O_ENTRY_D", O_ENTRY_D.column);
    pipelines.back()->current_columns.addColumn("O_CARRIER_ID", O_CARRIER_ID.column);
    auto result = executeSynchronouslyWithDefaultBreaker(db, std::move(pipelines), context);
    // return O_ID
    std::vector<std::shared_ptr<Batch>> batches;
    result->consumeBatches(batches, context.getWorkerId());
    return *reinterpret_cast<Identifier*>(batches.front()->getRow(0));
}

size_t runOSOrderlineSelect(DB& db, Identifier w_id, Identifier d_id, Identifier o_id, const ExecutionContext context) {
    // select OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D from TPCCH.ORDERLINE where OL_W_ID=? and OL_D_ID=? and OL_O_ID=?
    std::vector<std::unique_ptr<ExecutablePipeline>> pipelines;
    pipelines.push_back(std::make_unique<ExecutablePipeline>(
        0, db, "ORDERLINE",
        CompositeKey<4> { d_id, w_id, o_id, std::numeric_limits<Identifier>::min() },
        CompositeKey<4> { d_id, w_id, o_id, std::numeric_limits<Identifier>::max() },
        std::vector<NamedColumn>({ OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D }),
        context)
    );
    return executeSynchronouslyWithDefaultBreaker(db, std::move(pipelines), context)->getValidRowCount();
}

bool runOrderStatus(std::ostream& log, DB& db, Identifier w_id, Identifier d_id, bool customer_based_on_last_name, Identifier c_id, const std::string& c_last_in, const ExecutionContext context) {
    // BEGIN TRANSACTION
    std::string c_last;
	if (customer_based_on_last_name) {
        c_id = runOSCustomerSelect1(db, w_id, d_id, c_last_in, context);
        c_last = c_last_in;
    } else {
        c_last = runOSCustomerSelect2(db, c_id, w_id, d_id, context);
    }

    Identifier o_id = runOSOrderSelect(db, w_id, d_id, c_id, context);

    size_t orderline_count = runOSOrderlineSelect(db, w_id, d_id, o_id, context);

    log << "[OrderStatus] (" << w_id << ", " << d_id << ") " << c_last << " (" << c_id << ") order id " << o_id << ": " << orderline_count << " order lines" << std::endl;
    return true;
}

std::optional<Identifier> runDeliveryNeworderSelect(DB& db, Identifier w_id, Identifier d_id, const ExecutionContext context) {
    // select O_ID from NEWORDER where w_id = ? and d_id = ? order by O_ID asc limit 1
    std::vector<std::unique_ptr<ExecutablePipeline>> pipelines;
    pipelines.push_back(std::make_unique<ExecutablePipeline>(
        0, db, "NEWORDER",
        CompositeKey<3> { d_id, w_id, std::numeric_limits<Identifier>::min() },
        CompositeKey<3> { d_id, w_id, std::numeric_limits<Identifier>::max() },
        std::vector<NamedColumn>({ NO_O_ID }),
        context,
        1)
    );
    auto result = executeSynchronouslyWithDefaultBreaker(db, std::move(pipelines), context);
    if (result->getValidRowCount() == 0)
        return std::nullopt;
    std::vector<std::shared_ptr<Batch>> batches;
    result->consumeBatches(batches, context.getWorkerId());
    return std::make_optional(*reinterpret_cast<const Identifier*>(batches.front()->getRow(0)));
}

std::optional<std::pair<Identifier, Identifier>> runDeliveryOrderUpdate(DB& db, Identifier w_id, Identifier d_id, Identifier o_id, Identifier carrier_id, const ExecutionContext context) {
    // update ORDER set O_CARRIER_ID = ? where O_W_ID = ?, O_D_ID = ?, O_ID = ? returning O_C_ID, O_OL_CNT
    std::vector<std::unique_ptr<ExecutablePipeline>> pipelines;
    pipelines.push_back(std::make_unique<ExecutablePipeline>(
        0, db, "ORDER", CompositeKey<3> { d_id, w_id, o_id },
        std::vector<NamedColumn>({ O_CARRIER_ID, O_C_ID, O_OL_CNT }),
        std::vector<std::function<void(void*)>>({
            [carrier_id](void* data) { *reinterpret_cast<Identifier*>(data) = carrier_id; },
            [](void*) {},
            [](void*) {},
        }), context));
    auto result = executeSynchronouslyWithDefaultBreaker(db, std::move(pipelines), context);
    if (result->getValidRowCount() == 0)
        return std::nullopt;
    std::vector<std::shared_ptr<Batch>> batches;
    BatchDescription desc;
    result->consumeBatches(batches, context.getWorkerId());
    result->consumeBatchDescription(desc);
    const char* row = reinterpret_cast<const char*>(batches.front()->getRow(0));
    return std::make_optional(std::make_pair(
        *reinterpret_cast<const Identifier*>(row + desc.find("O_C_ID").offset),
        *reinterpret_cast<const Identifier*>(row + desc.find("O_OL_CNT").offset)
    ));
}

bool runDeliveryOrderlineValidation(DB& db, Identifier w_id, Identifier d_id, Identifier o_id, Identifier ol_cnt, const ExecutionContext context) {
    // scan ORDERLINE to check if an ORDERLINE with w_id, d_id, o_id, ol_number (= O_OL_CNT) exists
    std::vector<std::unique_ptr<ExecutablePipeline>> pipelines;
    pipelines.push_back(std::make_unique<ExecutablePipeline>(
        0, db, "ORDERLINE", CompositeKey<4> { d_id, w_id, o_id, ol_cnt },
        std::vector<NamedColumn>(),
        context));
    return executeSynchronouslyWithDefaultBreaker(db, std::move(pipelines), context)->getValidRowCount() != 0;
}

Decimal<2> runDeliveryOrderlineUpdate(DB& db, Identifier w_id, Identifier d_id, Identifier o_id, DateTime delivery_d, const ExecutionContext context) {
    // update ORDERLINE set OL_DELIVERY_D = ? where O_W_ID = ?, O_D_ID = ?, O_ID = ? returning sum(OL_AMOUNT)
    std::vector<std::unique_ptr<ExecutablePipeline>> pipelines;
    Decimal<2> total(0);
    pipelines.push_back(std::make_unique<ExecutablePipeline>(
        0, db, "ORDERLINE",
        CompositeKey<4> { d_id, w_id, o_id, std::numeric_limits<Identifier>::min() },
        CompositeKey<4> { d_id, w_id, o_id, std::numeric_limits<Identifier>::max() },
        std::vector<NamedColumn>({ OL_AMOUNT, OL_DELIVERY_D }),
        std::vector<std::function<void(void*)>>({
            [&total](void* data) { total += *reinterpret_cast<Decimal<2>*>(data); },
            [delivery_d](void* data) { *reinterpret_cast<DateTime*>(data) = delivery_d; }
        }), context));
    executeSynchronouslyWithDefaultBreaker(db, std::move(pipelines), context);
    return total;
}

void runDeliveryCustomerUpdate(DB& db, Identifier w_id, Identifier d_id, Identifier c_id, Decimal<2> ol_total, const ExecutionContext context) {
    // update CUSTOMER set C_BALANCE = C_BALANCE + <ol_total>, C_DELIVERY_CNT = C_DELIVERY_CNT + 1 where C_W_ID = ?, C_D_ID = ?, C_ID = ?
    std::vector<std::unique_ptr<ExecutablePipeline>> pipelines;
    pipelines.push_back(std::make_unique<ExecutablePipeline>(
        0, db, "CUSTOMER",
        CompositeKey<3> { d_id, w_id, c_id },
        CompositeKey<3> { d_id, w_id, c_id },
        std::vector<NamedColumn>({ C_BALANCE, C_DELIVERY_CNT }),
        std::vector<std::function<void(void*)>>({
            [ol_total](void* data) { *reinterpret_cast<Decimal<2>*>(data) += ol_total; },
            [](void* data) { *reinterpret_cast<Integer*>(data) += 1; }
        }), context));
    executeSynchronouslyWithDefaultBreaker(db, std::move(pipelines), context);
}

bool runDelivery(std::ostream& log, DB& db, Identifier w_id, Identifier carrier_id, DateTime ol_delivery_d, const ExecutionContext context) {
    for (Identifier d_id = 1; d_id <= 10; ++d_id) {
        // BEGIN TRANSACTION
        auto neworder_select_result = runDeliveryNeworderSelect(db, w_id, d_id, context);
        if (!neworder_select_result.has_value()) {
            log << "WARNING: Delivery skipped for warehouse " << w_id << ", district " << d_id << std::endl;
            continue;
        }
        Identifier o_id = neworder_select_result.value();

        // delete the w_id, d_id, o_id row from NEWORDER
        SharedGuard<TableBasepage> table(db.vmcache, db.getTableBasepageId("NEWORDER", context.getWorkerId()), context.getWorkerId());
        RowId rid = std::numeric_limits<RowId>::max();
        {
            BTree<CompositeKey<3>, size_t> pkey(db.vmcache, table->primary_key_index_basepage, context.getWorkerId());
            CompositeKey<3> key { d_id, w_id, o_id };
            auto it = pkey.lookupExact(key);
            if (it != pkey.end()) {
                rid = (*it).second;
                it.release();
                pkey.remove(key);
            }
        }
        if (rid != std::numeric_limits<RowId>::max()) {
            // mark row as deleted
            BTree<RowId, bool>(db.vmcache, table->visibility_basepage, context.getWorkerId()).latchForUpdate(rid).value().update(false);
        }

        auto order_update_result = runDeliveryOrderUpdate(db, w_id, d_id, o_id, carrier_id, context);
        // [manual isolation anomaly handling] if the order does not exist yet, continue
        if (!order_update_result.has_value())
            continue;
        Identifier c_id = order_update_result.value().first;
        Identifier ol_cnt = order_update_result.value().second;

        // [manual isolation anomaly handling] if not all orderlines for this order have been inserted yet, continue
        if (!runDeliveryOrderlineValidation(db, w_id, d_id, o_id, ol_cnt, context)) {
            //std::cout << "order incomplete" << std::endl;
            continue;
        }

        auto ol_total = runDeliveryOrderlineUpdate(db, w_id, d_id, o_id, ol_delivery_d, context);
        runDeliveryCustomerUpdate(db, w_id, d_id, c_id, ol_total, context);

        // note: the specification also permits delivering the orders across all districts in a single transaction, but this is irrelevant here as we do not have CC anyways
        // COMMIT
    }

    return true;
}

Identifier runSLDistrictSelect(DB& db, Identifier w_id, Identifier d_id, const ExecutionContext context) {
    // select D_NEXT_O_ID from TPCCH.DISTRICT where D_W_ID=? and D_ID=?
    std::vector<std::unique_ptr<ExecutablePipeline>> pipelines;
    pipelines.push_back(std::make_unique<ExecutablePipeline>(
        0, db, "DISTRICT", CompositeKey<2> { d_id, w_id },
        std::vector<NamedColumn>({ D_NEXT_O_ID }),
        context)
    );
    auto result = executeSynchronouslyWithDefaultBreaker(db, std::move(pipelines), context);
    std::vector<std::shared_ptr<Batch>> batches;
    result->consumeBatches(batches, context.getWorkerId());
    return *reinterpret_cast<Identifier*>(batches.front()->getRow(0));
}

int32_t runSLStockSelect(DB& db, Identifier w_id, Identifier d_id, int32_t d_next_oid, int32_t threshold, const ExecutionContext context) {
    // select count(*) from TPCCH.STOCK,(select distinct OL_I_ID from TPCCH.ORDERLINE where OL_W_ID=? and OL_D_ID=? and OL_O_ID<? and OL_O_ID>=?) where S_I_ID=OL_I_ID and S_W_ID=? and S_QUANTITY<?

    // query parameters
    uint32_t max_oid = d_next_oid;
    uint32_t min_oid = d_next_oid - 20;
    int32_t max_quantity = threshold;

    std::vector<std::unique_ptr<ExecutablePipeline>> pipelines1;
    // index scan for OL_I_ID on ORDERLINE OL_W_ID, OL_D_ID, OL_O_ID (between min_oid, max_oid)
    pipelines1.push_back(std::make_unique<ExecutablePipeline>(
        0, db, "ORDERLINE",
        CompositeKey<4> { d_id, w_id, min_oid, std::numeric_limits<Identifier>::min() },
        CompositeKey<4> { d_id, w_id, max_oid, std::numeric_limits<Identifier>::max() },
        std::vector<NamedColumn>({ OL_I_ID }),
        context)
    );
    auto result1 = executeSynchronouslyWithDefaultBreaker(db, std::move(pipelines1), context);
    // manually aggregate DISTINCT OL_I_ID
    std::vector<std::shared_ptr<Batch>> batches;
    result1->consumeBatches(batches, context.getWorkerId());
    std::vector<Identifier> i_ids;
    i_ids.reserve(300);
    for (auto& batch : batches) {
        for (auto it = batch->begin(); it < batch->end(); ++it) {
            i_ids.push_back(*reinterpret_cast<Identifier*>((*it).data));
        }
    }
    if (i_ids.size() > 300)
        std::cerr << "Warning: Got more OL_I_IDs (" << i_ids.size() << ") than expected (300) in runSLStockSelect()!" << std::endl;
    std::sort(i_ids.begin(), i_ids.end());
    auto last = std::unique(i_ids.begin(), i_ids.end());
    i_ids.erase(last, i_ids.end());

    // index lookup on STOCK S_I_ID (distinct I_IDs from prev op), S_W_ID (query param) with S_QUANTITY < max_quantity post-condition -> result = count of matching tuples
    std::vector<std::unique_ptr<ExecutablePipeline>> pipelines2;
    pipelines2.push_back(std::make_unique<ExecutablePipeline>(0));
    pipelines2.back()->addOperator(std::make_shared<SLStockSelectIndexScanOperator>(
        db, i_ids, w_id, max_quantity, context));
    pipelines2.back()->current_columns.addColumn(COUNT.name, COUNT.column);
    auto result2 = executeSynchronouslyWithDefaultBreaker(db, std::move(pipelines2), context);
    if (result2->getValidRowCount() != 1) {
        return 0;
    }
    batches.clear();
    result2->consumeBatches(batches, context.getWorkerId());
    return *reinterpret_cast<Integer*>(batches.front()->getRow(0));
}

bool runStockLevel(std::ostream& log, DB& db, Identifier w_id, Identifier d_id, int32_t threshold, const ExecutionContext context) {
	// BEGIN TRANSACTION
    int32_t d_next_oid = runSLDistrictSelect(db, w_id, d_id, context);
    size_t count = runSLStockSelect(db, w_id, d_id, d_next_oid, threshold, context);
    // COMMIT

    log << "[StockLevel] (" << w_id << ", " << d_id << ") " << d_next_oid << ", stock " << count << " (threshold " << threshold << ")" << std::endl;
    return true;
}

} // namespace tpcch