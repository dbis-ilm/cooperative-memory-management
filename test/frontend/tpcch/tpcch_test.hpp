#pragma once

#include "test/shared/db_test.hpp"

class DB;
class JobManager;
class ExecutionContext;

class TPCCHTestFixture : public DBTestFixture {
protected:
    void SetUp() override;
    void TearDown() override;
};
