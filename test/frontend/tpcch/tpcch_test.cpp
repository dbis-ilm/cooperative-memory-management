#include "tpcch_test.hpp"

#include "frontend/tpcch/tpcch.hpp"

void TPCCHTestFixture::SetUp() {
    DBTestFixture::SetUp();
    tpcch::createTables(*db, *context);
    tpcch::importFromCSV(*db, "../data/tpcch/1", *context);
    tpcch::createIndexes(*db, *context);
}

void TPCCHTestFixture::TearDown() {
    DBTestFixture::TearDown();
}

TEST_F(TPCCHTestFixture, import) {
    ASSERT_TRUE(tpcch::validateDatabase(*db, true));
}