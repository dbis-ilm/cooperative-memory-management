
#include "tpcch_test.hpp"

#include <fstream>

#include "prototype/execution/pipeline.hpp"
#include "prototype/execution/pipeline_breaker.hpp"
#include "frontend/tpcch/execution/os_order_select_index_scan.hpp"
#include "frontend/tpcch/tpcch.hpp"
#include "frontend/tpcch/transactions.hpp"

using namespace tpcch;

TEST_F(TPCCHTestFixture, StockLevel) {
    std::stringstream log;
    DateTime o_entry_d(0);
    runStockLevel(log, *db, 1, 1, 15, *context);
    // TODO: check for correct results
}

TEST_F(TPCCHTestFixture, OSOrderSelectIndexScan) {
    Identifier d_id = 10;
    Identifier w_id = 1;
    Identifier c_id = 2974;
    std::vector<std::unique_ptr<ExecutablePipeline>> pipelines;
    pipelines.push_back(std::make_unique<ExecutablePipeline>(pipelines.size()));
    pipelines.back()->addOperator(std::make_shared<OSOrderSelectIndexScanOperator>(
        *db,
        CompositeKey<4> { d_id, w_id, c_id, std::numeric_limits<Identifier>::min() },
        CompositeKey<4> { d_id, w_id, c_id, std::numeric_limits<Identifier>::max() },
        std::vector<NamedColumn>({ O_ID, O_ENTRY_D, O_CARRIER_ID }),
        *context));
    pipelines.back()->current_columns.addColumn("O_ID", O_ID.column);
    pipelines.back()->current_columns.addColumn("O_ENTRY_D", O_ENTRY_D.column);
    pipelines.back()->current_columns.addColumn("O_CARRIER_ID", O_CARRIER_ID.column);
    auto result = executeSynchronouslyWithDefaultBreaker(*db, std::move(pipelines), *context);
    std::vector<std::shared_ptr<Batch>> batches;
    result->consumeBatches(batches, context->getWorkerId());
    EXPECT_EQ(result->getValidRowCount(), 1);
    const char* result_row = reinterpret_cast<const char*>(batches.front()->getRow(0));
    EXPECT_EQ(*reinterpret_cast<const Identifier*>(result_row), 1703);
}