#pragma once

#include <fcntl.h>
#include <gtest/gtest.h>

class DB;
class JobManager;
class ExecutionContext;

class DBTestFixture : public ::testing::Test {
public:
    std::shared_ptr<DB> db;
    std::shared_ptr<JobManager> job_manager;
    std::shared_ptr<ExecutionContext> context;

protected:
    void SetUp() override;
    void TearDown() override;

private:
    std::string path;
    std::string lock_path;
    int lock_fd;
};
