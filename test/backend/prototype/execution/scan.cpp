#include "test/shared/db_test.hpp"
#include "prototype/execution/pipeline.hpp"
#include "prototype/execution/qep.hpp"
#include "prototype/execution/scan.hpp"
#include "prototype/execution/table_column.hpp"
#include "prototype/scheduling/job_manager.hpp"
#include "prototype/storage/persistence/btree.hpp"
#include "prototype/storage/persistence/table.hpp"
#include "prototype/utils/validation.hpp"

class ScanFixture : public DBTestFixture {
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
        BTree<RowId, bool> visibility(db->vmcache, t1_basepage->visibility_basepage, 0);
        for (size_t i = 0; i < t1c1_values.size(); ++i)
            visibility.insertNext(i != 2); // mark row 2 (56, 33, 6) as deleted
    }
};

TEST_F(ScanFixture, full_scan) {
    std::vector<std::unique_ptr<ExecutablePipeline>> pipelines;
    pipelines.push_back(std::make_unique<ExecutablePipeline>(pipelines.size(), *db, "T1", std::vector<NamedColumn>({ c1, c2, c3 }), *context));
    pipelines.back()->addDefaultBreaker(*context);
    auto qep = std::make_shared<QEP>(std::move(pipelines));

    // execute
    qep->begin(*context);
    qep->waitForExecution(*context, db->vmcache);

    // validate results
    BatchVector expected_result(db->vmcache, 3 * sizeof(Identifier));
    Identifier* row = reinterpret_cast<Identifier*>(expected_result.addRow());
    row[0] = 51; row[1] = 11; row[2] = 11;
    row = reinterpret_cast<Identifier*>(expected_result.addRow());
    row[0] = 2; row[1] = 22; row[2] = 15;
    row = reinterpret_cast<Identifier*>(expected_result.addRow());
    row[0] = 3; row[1] = 44; row[2] = 11;
    row = reinterpret_cast<Identifier*>(expected_result.addRow());
    row[0] = 41; row[1] = 55; row[2] = 6;
    EXPECT_TRUE(validateQueryResult(qep->getResult(), expected_result, false));
}

TEST_F(ScanFixture, filtering_scan) {
    std::vector<std::unique_ptr<ExecutablePipeline>> pipelines;
    // filter: c3 == 6
    pipelines.push_back(std::make_unique<ExecutablePipeline>(pipelines.size(), *db, "T1", std::vector<NamedColumn>({ c3 }), std::vector<uint32_t>({ 6 }), std::vector<NamedColumn>({ c1, c2, c3 }), *context));
    pipelines.back()->addDefaultBreaker(*context);
    auto qep = std::make_shared<QEP>(std::move(pipelines));

    // execute
    qep->begin(*context);
    qep->waitForExecution(*context, db->vmcache);

    // validate results
    BatchVector expected_result(db->vmcache, 3 * sizeof(Identifier));
    Identifier* row = reinterpret_cast<Identifier*>(expected_result.addRow());
    row[0] = 41; row[1] = 55; row[2] = 6;
    EXPECT_TRUE(validateQueryResult(qep->getResult(), expected_result, false));
}