#include "test/shared/db_test.hpp"
#include "prototype/scheduling/execution_context.hpp"

class DBFixture : public DBTestFixture { };

TEST_F(DBFixture, getNumTables) {
    uint64_t schema_id = db->createSchema("TEST", context->getWorkerId());
    EXPECT_EQ(db->getNumTables(context->getWorkerId()), 0);
    db->createTable(schema_id, "TABLE_A", 2, context->getWorkerId());
    EXPECT_EQ(db->getNumTables(context->getWorkerId()), 1);
    db->createTable(schema_id, "TABLE_B", 6, context->getWorkerId());
    EXPECT_EQ(db->getNumTables(context->getWorkerId()), 2);
}

TEST_F(DBFixture, getTableBasepageId) {
    uint64_t schema_id = db->createSchema("TEST", context->getWorkerId());
    uint64_t tid_a = db->createTable(schema_id, "TABLE_A", 2, context->getWorkerId());
    uint64_t tid_b = db->createTable(schema_id, "TABLE_B", 6, context->getWorkerId());

    EXPECT_NO_THROW(db->getTableBasepageId(tid_a, context->getWorkerId()));
    EXPECT_NO_THROW(db->getTableBasepageId(tid_b, context->getWorkerId()));
    EXPECT_ANY_THROW(db->getTableBasepageId(0xdeadbeef, context->getWorkerId()));

    EXPECT_NO_THROW(db->getTableBasepageId("TABLE_A", context->getWorkerId()));
    EXPECT_NO_THROW(db->getTableBasepageId("TABLE_B", context->getWorkerId()));
    EXPECT_ANY_THROW(db->getTableBasepageId("QWERTZ", context->getWorkerId()));

    EXPECT_EQ(
        db->getTableBasepageId(tid_a, context->getWorkerId()),
        db->getTableBasepageId("TABLE_A", context->getWorkerId())
    );
    EXPECT_EQ(
        db->getTableBasepageId(tid_b, context->getWorkerId()),
        db->getTableBasepageId("TABLE_B", context->getWorkerId())
    );
}