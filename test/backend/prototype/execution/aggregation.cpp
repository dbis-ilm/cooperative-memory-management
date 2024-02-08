#include "test/shared/db_test.hpp"
#include "prototype/execution/pipeline.hpp"
#include "prototype/execution/aggregation.hpp"
#include "prototype/execution/scan.hpp"
#include "prototype/execution/table_column.hpp"
#include "prototype/execution/qep.hpp"
#include "prototype/scheduling/job_manager.hpp"
#include "prototype/utils/print_result.hpp"
#include "prototype/utils/validation.hpp"

class AggregationFixture : public DBTestFixture {
public:
    const NamedColumn t1c1 = NamedColumn(std::string("t1.c1"), std::make_shared<UnencodedTableColumn<Identifier>>(0));

protected:
    void SetUp() override {
        DBTestFixture::SetUp();

        // create some test data
        uint64_t t1_tid = db->createTable(db->default_schema_id, "T1", 1, 0);
        PageId t1_basepage_pid = db->getTableBasepageId(t1_tid, 0);
        ExclusiveGuard<TableBasepage> t1_basepage(db->vmcache, t1_basepage_pid, 0);
        std::vector<Identifier> t1c1_values({ 1, 2, 1, 2, 2, 4, 5 });
        db->appendValues<Identifier>(0, t1_basepage->column_basepages[0], t1c1_values.begin(), t1c1_values.end(), 0);
        BTree<RowId, bool> visibility(db->vmcache, t1_basepage->visibility_basepage, 0);
        for (size_t i = 0; i < t1c1_values.size(); ++i)
            visibility.insertNext(true);
    }

};


TEST_F(AggregationFixture, distinct) {
    std::vector<std::unique_ptr<ExecutablePipeline>> pipelines;

    // scan t1 and perform thread-local pre-aggregation
    pipelines.push_back(std::make_unique<ExecutablePipeline>(pipelines.size(), *db, "T1", std::vector<NamedColumn>({ t1c1 }), *context));
    pipelines.back()->addAggregationBreaker(db->vmcache, sizeof(Identifier), *context);

    // aggregate partition-wise
    pipelines.push_back(std::make_unique<ExecutablePipeline>(pipelines.size()));
    pipelines.back()->addAggregation(db->vmcache, *pipelines[0]);
    pipelines.back()->addDefaultBreaker(*context);
    auto qep = std::make_shared<QEP>(std::move(pipelines));

    // execute
    qep->begin(*context);
    qep->waitForExecution(*context, db->vmcache);

    // validate results
    BatchVector expected_result(db->vmcache, sizeof(Identifier));
    uint32_t* row = reinterpret_cast<uint32_t*>(expected_result.addRow());
    row[0] = 1;
    row = reinterpret_cast<uint32_t*>(expected_result.addRow());
    row[0] = 2;
    row = reinterpret_cast<uint32_t*>(expected_result.addRow());
    row[0] = 4;
    row = reinterpret_cast<uint32_t*>(expected_result.addRow());
    row[0] = 5;
    EXPECT_TRUE(validateQueryResult(qep->getResult(), expected_result));
}