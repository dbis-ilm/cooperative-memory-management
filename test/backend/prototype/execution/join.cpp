#include "test/shared/db_test.hpp"
#include "prototype/execution/pipeline.hpp"
#include "prototype/execution/join.hpp"
#include "prototype/execution/scan.hpp"
#include "prototype/execution/table_column.hpp"
#include "prototype/execution/qep.hpp"
#include "prototype/scheduling/job_manager.hpp"
#include "prototype/utils/print_result.hpp"
#include "prototype/utils/validation.hpp"

class JoinFixture : public DBTestFixture {
public:
    const NamedColumn t1c1 = NamedColumn(std::string("t1.c1"), std::make_shared<UnencodedTableColumn<Identifier>>(0));
    const NamedColumn t1c2 = NamedColumn(std::string("t1.c2"), std::make_shared<UnencodedTableColumn<Identifier>>(1));
    const NamedColumn t1c3 = NamedColumn(std::string("t1.c3"), std::make_shared<UnencodedTableColumn<Identifier>>(2));
    const NamedColumn t2c1 = NamedColumn(std::string("t2.c1"), std::make_shared<UnencodedTableColumn<Identifier>>(0));
    const NamedColumn t2c2 = NamedColumn(std::string("t2.c2"), std::make_shared<UnencodedTableColumn<Integer>>(1));
    const NamedColumn t3c1 = NamedColumn(std::string("t3.c1"), std::make_shared<UnencodedTableColumn<Identifier>>(0));
    const NamedColumn t3c2 = NamedColumn(std::string("t3.c2"), std::make_shared<UnencodedTableColumn<Identifier>>(1));
    const NamedColumn t3c3 = NamedColumn(std::string("t3.c3"), std::make_shared<UnencodedTableColumn<Integer>>(2));
    const NamedColumn t3c4 = NamedColumn(std::string("t3.c4"), std::make_shared<UnencodedTableColumn<Identifier>>(3));

protected:
    void SetUp() override {
        DBTestFixture::SetUp();

        // create some test data
        uint64_t t1_tid = db->createTable(db->default_schema_id, "T1", 3, 0);
        uint64_t t2_tid = db->createTable(db->default_schema_id, "T2", 2, 0);
        uint64_t t3_tid = db->createTable(db->default_schema_id, "T3", 4, 0);
        PageId t1_basepage_pid = db->getTableBasepageId(t1_tid, 0);
        ExclusiveGuard<TableBasepage> t1_basepage(db->vmcache, t1_basepage_pid, 0);
        PageId t2_basepage_pid = db->getTableBasepageId(t2_tid, 0);
        ExclusiveGuard<TableBasepage> t2_basepage(db->vmcache, t2_basepage_pid, 0);
        PageId t3_basepage_pid = db->getTableBasepageId(t3_tid, 0);
        ExclusiveGuard<TableBasepage> t3_basepage(db->vmcache, t3_basepage_pid, 0);
        std::vector<Identifier> t1c1_values({ 1, 2, 3, 4, 5 });
        std::vector<Identifier> t1c2_values({ 11, 22, 33, 44, 55 });
        std::vector<Identifier> t1c3_values({ 9, 435, 213, 24, 12 });
        std::vector<Identifier> t2c1_values({ 1, 2, 2, 6, 5, 2, 5, 7, 1 });
        std::vector<Integer> t2c2_values({ -11, -22, -33, -44, -55, -66, -77, -88, -99 });
        std::vector<Identifier> t3c1_values({ 1, 2, 3, 4, 5 });
        std::vector<Identifier> t3c2_values({ 11, 44, 22, 33, 55 });
        std::vector<Integer> t3c3_values({ -5, -4, -3, -2, -1 });
        std::vector<Identifier> t3c4_values({ 11, 435, 22, 33, 12 });
        db->appendValues<Identifier>(0, t1_basepage->column_basepages[0], t1c1_values.begin(), t1c1_values.end(), 0);
        db->appendValues<Identifier>(0, t1_basepage->column_basepages[1], t1c2_values.begin(), t1c2_values.end(), 0);
        db->appendValues<Identifier>(0, t1_basepage->column_basepages[2], t1c3_values.begin(), t1c3_values.end(), 0);
        db->appendValues<Identifier>(0, t2_basepage->column_basepages[0], t2c1_values.begin(), t2c1_values.end(), 0);
        db->appendValues<Integer>(0, t2_basepage->column_basepages[1], t2c2_values.begin(), t2c2_values.end(), 0);
        db->appendValues<Identifier>(0, t3_basepage->column_basepages[0], t3c1_values.begin(), t3c1_values.end(), 0);
        db->appendValues<Identifier>(0, t3_basepage->column_basepages[1], t3c2_values.begin(), t3c2_values.end(), 0);
        db->appendValues<Integer>(0, t3_basepage->column_basepages[2], t3c3_values.begin(), t3c3_values.end(), 0);
        db->appendValues<Identifier>(0, t3_basepage->column_basepages[3], t3c4_values.begin(), t3c4_values.end(), 0);
        BTree<RowId, bool> t1_visibility(db->vmcache, t1_basepage->visibility_basepage, 0);
        for (size_t i = 0; i < t1c1_values.size(); ++i)
            t1_visibility.insertNext(true);
        BTree<RowId, bool> t2_visibility(db->vmcache, t2_basepage->visibility_basepage, 0);
        for (size_t i = 0; i < t2c1_values.size(); ++i)
            t2_visibility.insertNext(true);
        BTree<RowId, bool> t3_visibility(db->vmcache, t3_basepage->visibility_basepage, 0);
        for (size_t i = 0; i < t3c1_values.size(); ++i)
            t3_visibility.insertNext(true);
    }

};


TEST_F(JoinFixture, join_distinct_build_side) {
    std::vector<std::unique_ptr<ExecutablePipeline>> pipelines;

    // scan t1 and collect tuples
    pipelines.push_back(std::make_unique<ExecutablePipeline>(0, *db, "T1", std::vector<NamedColumn>({ t1c1, t1c2 }), *context));
    pipelines[0]->addJoinBreaker(db->vmcache, *context);

    // init & build hash table
    auto join_build = JoinFactory::createBuildPipelines(pipelines, db->vmcache, *pipelines[0], t1c1.column->getValueTypeSize());

    // scan t2 and probe hash table
    pipelines.push_back(std::make_unique<ExecutablePipeline>(3, *db, "T2", std::vector<NamedColumn>({ t2c1, t2c2 }), *context));
    pipelines[3]->addJoinProbe(db->vmcache, *pipelines[2], std::vector<NamedColumn>({ t1c1, t1c2, t2c2 }));
    pipelines[3]->addDefaultBreaker(*context);
    auto qep = std::make_shared<QEP>(std::move(pipelines));

    // execute
    qep->begin(*context);
    qep->waitForExecution(*context, db->vmcache);

    // validate results
    BatchVector expected_result(db->vmcache, sizeof(Identifier) + 2 * sizeof(Integer));
    uint32_t* row = reinterpret_cast<uint32_t*>(expected_result.addRow());
    row[0] = 1; row[1] = 11; row[2] = -11;
    row = reinterpret_cast<uint32_t*>(expected_result.addRow());
    row[0] = 1; row[1] = 11; row[2] = -99;
    row = reinterpret_cast<uint32_t*>(expected_result.addRow());
    row[0] = 2; row[1] = 22; row[2] = -22;
    row = reinterpret_cast<uint32_t*>(expected_result.addRow());
    row[0] = 2; row[1] = 22; row[2] = -33;
    row = reinterpret_cast<uint32_t*>(expected_result.addRow());
    row[0] = 2; row[1] = 22; row[2] = -66;
    row = reinterpret_cast<uint32_t*>(expected_result.addRow());
    row[0] = 5; row[1] = 55; row[2] = -55;
    row = reinterpret_cast<uint32_t*>(expected_result.addRow());
    row[0] = 5; row[1] = 55; row[2] = -77;
    EXPECT_TRUE(validateQueryResult(qep->getResult(), expected_result));
}

TEST_F(JoinFixture, join_non_distinct_build_side) {
    std::vector<std::unique_ptr<ExecutablePipeline>> pipelines;

    // scan t2 and collect tuples
    pipelines.push_back(std::make_unique<ExecutablePipeline>(0, *db, "T2", std::vector<NamedColumn>({ t2c1, t2c2 }), *context));
    pipelines[0]->addJoinBreaker(db->vmcache, *context);

    // init & build hash table
    auto join_build = JoinFactory::createBuildPipelines(pipelines, db->vmcache, *pipelines[0], t1c1.column->getValueTypeSize());

    // scan t1 and probe hash table
    pipelines.push_back(std::make_unique<ExecutablePipeline>(3, *db, "T1", std::vector<NamedColumn>({ t1c1, t1c2 }), *context));
    pipelines[3]->addJoinProbe(db->vmcache, *pipelines[2], std::vector<NamedColumn>({ t1c1, t2c2, t1c2 }));
    pipelines[3]->addDefaultBreaker(*context);
    auto qep = std::make_shared<QEP>(std::move(pipelines));

    // execute
    qep->begin(*context);
    qep->waitForExecution(*context, db->vmcache);

    // validate results
    BatchVector expected_result(db->vmcache, sizeof(Identifier) + 2 * sizeof(Integer));
    uint32_t* row = reinterpret_cast<uint32_t*>(expected_result.addRow());
    row[0] = 1; row[1] = -11; row[2] = 11;
    row = reinterpret_cast<uint32_t*>(expected_result.addRow());
    row[0] = 1; row[1] = -99; row[2] = 11;
    row = reinterpret_cast<uint32_t*>(expected_result.addRow());
    row[0] = 2; row[1] = -22; row[2] = 22;
    row = reinterpret_cast<uint32_t*>(expected_result.addRow());
    row[0] = 2; row[1] = -33; row[2] = 22;
    row = reinterpret_cast<uint32_t*>(expected_result.addRow());
    row[0] = 2; row[1] = -66; row[2] = 22;
    row = reinterpret_cast<uint32_t*>(expected_result.addRow());
    row[0] = 5; row[1] = -55; row[2] = 55;
    row = reinterpret_cast<uint32_t*>(expected_result.addRow());
    row[0] = 5; row[1] = -77; row[2] = 55;
    EXPECT_TRUE(validateQueryResult(qep->getResult(), expected_result));
}

TEST_F(JoinFixture, join_composite_key_8B) {
    std::vector<std::unique_ptr<ExecutablePipeline>> pipelines;

    // scan t1 and collect tuples
    pipelines.push_back(std::make_unique<ExecutablePipeline>(0, *db, "T1", std::vector<NamedColumn>({ t1c1, t1c2 }), *context));
    pipelines[0]->addJoinBreaker(db->vmcache, *context);

    // init & build hash table
    auto join_build = JoinFactory::createBuildPipelines(pipelines, db->vmcache, *pipelines[0], t1c1.column->getValueTypeSize() + t1c2.column->getValueTypeSize());

    // scan t3 and probe hash table
    pipelines.push_back(std::make_unique<ExecutablePipeline>(3, *db, "T3", std::vector<NamedColumn>({ t3c1, t3c2, t3c3 }), *context));
    BatchDescription output_desc = BatchDescription();
    pipelines[3]->addJoinProbe(db->vmcache, *pipelines[2], std::vector<NamedColumn>({ t1c1, t1c2, t3c3 }));
    pipelines[3]->addDefaultBreaker(*context);
    auto qep = std::make_shared<QEP>(std::move(pipelines));

    // execute
    qep->begin(*context);
    qep->waitForExecution(*context, db->vmcache);

    // validate results
    BatchVector expected_result(db->vmcache, 2 * sizeof(Identifier) + sizeof(Integer));
    uint32_t* row = reinterpret_cast<uint32_t*>(expected_result.addRow());
    row[0] = 1; row[1] = 11; row[2] = -5;
    row = reinterpret_cast<uint32_t*>(expected_result.addRow());
    row[0] = 5; row[1] = 55; row[2] = -1;
    EXPECT_TRUE(validateQueryResult(qep->getResult(), expected_result));
}

TEST_F(JoinFixture, join_composite_key_12B) {
    std::vector<std::unique_ptr<ExecutablePipeline>> pipelines;

    // scan t1 and collect tuples
    pipelines.push_back(std::make_unique<ExecutablePipeline>(0, *db, "T1", std::vector<NamedColumn>({ t1c1, t1c2, t1c3 }), *context));
    pipelines[0]->addJoinBreaker(db->vmcache, *context);

    // init & build hash table
    auto join_build = JoinFactory::createBuildPipelines(pipelines, db->vmcache, *pipelines[0], t1c1.column->getValueTypeSize() + t1c2.column->getValueTypeSize() + t1c3.column->getValueTypeSize());

    // scan t3 and probe hash table
    pipelines.push_back(std::make_unique<ExecutablePipeline>(3, *db, "T3", std::vector<NamedColumn>({ t3c1, t3c2, t3c4, t3c3 }), *context));
    BatchDescription output_desc = BatchDescription();
    pipelines[3]->addJoinProbe(db->vmcache, *pipelines[2], std::vector<NamedColumn>({ t1c1, t1c2, t3c4, t3c3 }));
    pipelines[3]->addDefaultBreaker(*context);
    auto qep = std::make_shared<QEP>(std::move(pipelines));

    // execute
    qep->begin(*context);
    qep->waitForExecution(*context, db->vmcache);

    // validate results
    BatchVector expected_result(db->vmcache, 3 * sizeof(Identifier) + sizeof(Integer));
    uint32_t* row = reinterpret_cast<uint32_t*>(expected_result.addRow());
    row[0] = 5; row[1] = 55; row[2] = 12; row[3] = -1;
    EXPECT_TRUE(validateQueryResult(qep->getResult(), expected_result));
}