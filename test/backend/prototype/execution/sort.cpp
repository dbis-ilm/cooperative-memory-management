#include "test/shared/db_test.hpp"
#include "prototype/execution/pipeline.hpp"
#include "prototype/execution/qep.hpp"
#include "prototype/execution/sort.hpp"
#include "prototype/execution/table_column.hpp"
#include "prototype/scheduling/job_manager.hpp"
#include "prototype/storage/persistence/btree.hpp"
#include "prototype/storage/persistence/table.hpp"
#include "prototype/utils/validation.hpp"

class SortFixture : public DBTestFixture {
public:
    const NamedColumn c1 = NamedColumn(std::string("c1"), std::make_shared<UnencodedTableColumn<Identifier>>(0));
    const NamedColumn c2 = NamedColumn(std::string("c2"), std::make_shared<UnencodedTableColumn<Identifier>>(1));
    const NamedColumn c3 = NamedColumn(std::string("c3"), std::make_shared<UnencodedTableColumn<Identifier>>(2));

protected:
    void SetUp() override {
        DBTestFixture::SetUp();

        // create some test data
        uint64_t t1_tid = db->createTable(db->default_schema_id, "T1", 3, 0);
        PageId t1_basepage_pid = db->getTableBasepageId(t1_tid, 0);
        ExclusiveGuard<TableBasepage> t1_basepage(db->vmcache, t1_basepage_pid, 0);
        std::vector<Identifier> t1c1_values({ 51, 2, 56, 3, 41 });
        std::vector<Identifier> t1c2_values({ 11, 22, 33, 44, 55 });
        std::vector<Identifier> t1c3_values({ 11, 15, 6, 11, 6 });
        db->appendValues<Identifier>(0, t1_basepage->column_basepages[0], t1c1_values.begin(), t1c1_values.end(), 0);
        db->appendValues<Identifier>(0, t1_basepage->column_basepages[1], t1c2_values.begin(), t1c2_values.end(), 0);
        db->appendValues<Identifier>(0, t1_basepage->column_basepages[2], t1c3_values.begin(), t1c3_values.end(), 0);
        BTree<RowId, bool> t1_visibility(db->vmcache, t1_basepage->visibility_basepage, 0);
        for (size_t i = 0; i < t1c1_values.size(); ++i)
            t1_visibility.insertNext(true);

        uint64_t t2_tid = db->createTable(db->default_schema_id, "T2", 1, 0);
        PageId t2_basepage_pid = db->getTableBasepageId(t2_tid, 0);
        ExclusiveGuard<TableBasepage> t2_basepage(db->vmcache, t2_basepage_pid, 0);
        std::vector<Identifier> t2c1_values;
        const size_t t2_cardinality = 4096;
        t2c1_values.reserve(t2_cardinality);
        for (size_t i = 0; i < t2_cardinality; ++i)
            t2c1_values.push_back(t2_cardinality - i);
        db->appendValues<Identifier>(0, t2_basepage->column_basepages[0], t2c1_values.begin(), t2c1_values.end(), 0);
        BTree<RowId, bool> t2_visibility(db->vmcache, t2_basepage->visibility_basepage, 0);
        for (size_t i = 0; i < t2c1_values.size(); ++i)
            t2_visibility.insertNext(true);

    }

};

TEST_F(SortFixture, sort_single_key_ascending) {
    std::vector<std::unique_ptr<ExecutablePipeline>> pipelines;
    pipelines.push_back(std::make_unique<ExecutablePipeline>(pipelines.size(), *db, "T1", std::vector<NamedColumn>({ c1, c2 }), *context));
    pipelines.back()->addSortBreaker(std::vector<NamedColumn>({ c1 }), std::vector<Order>({ Order::Ascending }), context->getWorkerCount());
    pipelines.push_back(std::make_unique<ExecutablePipeline>(pipelines.size()));
    pipelines.back()->addSort(db->vmcache, *pipelines[0].get());
    pipelines.back()->addDefaultBreaker(*context);
    auto qep = std::make_shared<QEP>(std::move(pipelines));

    // execute
    qep->begin(*context);
    qep->waitForExecution(*context, db->vmcache);

    // validate results
    BatchVector expected_result(db->vmcache, 2 * sizeof(Identifier));
    Identifier* row = reinterpret_cast<Identifier*>(expected_result.addRow());
    row[0] = 2; row[1] = 22;
    row = reinterpret_cast<Identifier*>(expected_result.addRow());
    row[0] = 3; row[1] = 44;
    row = reinterpret_cast<Identifier*>(expected_result.addRow());
    row[0] = 41; row[1] = 55;
    row = reinterpret_cast<Identifier*>(expected_result.addRow());
    row[0] = 51; row[1] = 11;
    row = reinterpret_cast<Identifier*>(expected_result.addRow());
    row[0] = 56; row[1] = 33;
    EXPECT_TRUE(validateQueryResult(qep->getResult(), expected_result, true));
}

TEST_F(SortFixture, sort_single_key_descending) {
    std::vector<std::unique_ptr<ExecutablePipeline>> pipelines;
    pipelines.push_back(std::make_unique<ExecutablePipeline>(pipelines.size(), *db, "T1", std::vector<NamedColumn>({ c1, c2 }), *context));
    pipelines.back()->addSortBreaker(std::vector<NamedColumn>({ c1 }), std::vector<Order>({ Order::Descending }), context->getWorkerCount());
    pipelines.push_back(std::make_unique<ExecutablePipeline>(pipelines.size()));
    pipelines.back()->addSort(db->vmcache, *pipelines[0].get());
    pipelines.back()->addDefaultBreaker(*context);
    auto qep = std::make_shared<QEP>(std::move(pipelines));

    // execute
    qep->begin(*context);
    qep->waitForExecution(*context, db->vmcache);

    // validate results
    BatchVector expected_result(db->vmcache, 2 * sizeof(Identifier));
    Identifier* row = reinterpret_cast<Identifier*>(expected_result.addRow());
    row[0] = 56; row[1] = 33;
    row = reinterpret_cast<Identifier*>(expected_result.addRow());
    row[0] = 51; row[1] = 11;
    row = reinterpret_cast<Identifier*>(expected_result.addRow());
    row[0] = 41; row[1] = 55;
    row = reinterpret_cast<Identifier*>(expected_result.addRow());
    row[0] = 3; row[1] = 44;
    row = reinterpret_cast<Identifier*>(expected_result.addRow());
    row[0] = 2; row[1] = 22;
    EXPECT_TRUE(validateQueryResult(qep->getResult(), expected_result, true));
}

TEST_F(SortFixture, sort_single_key_ascending_duplicates) {
    std::vector<std::unique_ptr<ExecutablePipeline>> pipelines;
    pipelines.push_back(std::make_unique<ExecutablePipeline>(pipelines.size(), *db, "T1", std::vector<NamedColumn>({ c3 }), *context));
    pipelines.back()->addSortBreaker(std::vector<NamedColumn>({ c3 }), std::vector<Order>({ Order::Ascending }), context->getWorkerCount());
    pipelines.push_back(std::make_unique<ExecutablePipeline>(pipelines.size()));
    pipelines.back()->addSort(db->vmcache, *pipelines[0].get());
    pipelines.back()->addDefaultBreaker(*context);
    auto qep = std::make_shared<QEP>(std::move(pipelines));

    // execute
    qep->begin(*context);
    qep->waitForExecution(*context, db->vmcache);

    // validate results
    std::vector<std::shared_ptr<Batch>> batches;
    qep->getResult()->consumeBatches(batches, context->getWorkerId());
    int32_t last_val = 0;
    for (auto& batch : batches) {
        for (auto it = batch->begin(); it < batch->end(); it++) {
            int32_t val = *reinterpret_cast<int32_t*>((*it).data);
            EXPECT_LE(last_val, val);
            last_val = val;
        }
    }
}

TEST_F(SortFixture, sort_4096) {
    std::vector<std::unique_ptr<ExecutablePipeline>> pipelines;
    pipelines.push_back(std::make_unique<ExecutablePipeline>(pipelines.size(), *db, "T2", std::vector<NamedColumn>({ c1 }), *context));
    pipelines.back()->addSortBreaker(std::vector<NamedColumn>({ c1 }), std::vector<Order>({ Order::Ascending }), context->getWorkerCount());
    pipelines.push_back(std::make_unique<ExecutablePipeline>(pipelines.size()));
    pipelines.back()->addSort(db->vmcache, *pipelines[0].get());
    pipelines.back()->addDefaultBreaker(*context);
    auto qep = std::make_shared<QEP>(std::move(pipelines));

    // execute
    qep->begin(*context);
    qep->waitForExecution(*context, db->vmcache);

    // validate results
    std::vector<std::shared_ptr<Batch>> batches;
    qep->getResult()->consumeBatches(batches, context->getWorkerId());
    int32_t last_val = 0;
    size_t num_result_rows = 0;
    for (auto& batch : batches) {
        for (auto it = batch->begin(); it < batch->end(); it++) {
            int32_t val = *reinterpret_cast<int32_t*>((*it).data);
            EXPECT_LT(last_val, val);
            last_val = val;
            num_result_rows++;
        }
    }
    EXPECT_EQ(num_result_rows, 4096);
}
