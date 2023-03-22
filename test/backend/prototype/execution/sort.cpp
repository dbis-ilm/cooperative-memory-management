#include "../core/db_test.hpp"
#include "prototype/execution/pipeline.hpp"
#include "prototype/execution/qep.hpp"
#include "prototype/execution/sort.hpp"
#include "prototype/scheduling/job_manager.hpp"
#include "prototype/storage/persistence/table.hpp"
#include "prototype/utils/validation.hpp"

class SortFixture : public DBTestFixture {
public:
    const NamedColumn c1 = NamedColumn(std::string("c1"), std::make_shared<UnencodedTemporaryColumn<Identifier>>());
    const NamedColumn c2 = NamedColumn(std::string("c2"), std::make_shared<UnencodedTemporaryColumn<Identifier>>());
    const NamedColumn c3 = NamedColumn(std::string("c3"), std::make_shared<UnencodedTemporaryColumn<Identifier>>());

protected:
    void SetUp() override {
        DBTestFixture::SetUp();

        // create some test data
        uint64_t t1_tid = db->createTable(db->default_schema_id, "T1", 3, 0);
        PageId t1_basepage_pid = db->getTableBasepageId(t1_tid, 0);
        TableBasepage* t1_basepage = reinterpret_cast<TableBasepage*>(db->vmcache.fixExclusive(t1_basepage_pid, 0));
        std::vector<Identifier> t1c1_values({ 51, 2, 56, 3, 41 });
        std::vector<Identifier> t1c2_values({ 11, 22, 33, 44, 55 });
        std::vector<Identifier> t1c3_values({ 11, 15, 6, 11, 6 });
        db->appendValues<Identifier>(0, t1_basepage->column_basepages[0], t1c1_values.begin(), t1c1_values.end(), 0);
        db->appendValues<Identifier>(0, t1_basepage->column_basepages[1], t1c2_values.begin(), t1c2_values.end(), 0);
        db->appendValues<Identifier>(0, t1_basepage->column_basepages[2], t1c3_values.begin(), t1c3_values.end(), 0);
        t1_basepage->cardinality = t1c1_values.size();
        db->vmcache.unfixExclusive(t1_basepage_pid);
    }

};

TEST_F(SortFixture, sort_single_key_ascending) {
    std::vector<std::unique_ptr<ExecutablePipeline>> pipelines;
    pipelines.push_back(std::make_unique<ExecutablePipeline>(pipelines.size(), *db, "T1", std::vector<uint64_t>({ 0, 1 }), std::vector<NamedColumn>({ c1, c2 }), *context));
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
    pipelines.push_back(std::make_unique<ExecutablePipeline>(pipelines.size(), *db, "T1", std::vector<uint64_t>({ 0, 1 }), std::vector<NamedColumn>({ c1, c2 }), *context));
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
    pipelines.push_back(std::make_unique<ExecutablePipeline>(pipelines.size(), *db, "T1", std::vector<uint64_t>({ 2 }), std::vector<NamedColumn>({ c3 }), *context));
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
