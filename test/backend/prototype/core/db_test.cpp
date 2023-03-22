#include "db_test.hpp"

#include "prototype/core/db.hpp"
#include "prototype/core/types.hpp"
#include "prototype/scheduling/execution_context.hpp"

void DBTestFixture::SetUp() {
    job_manager = std::make_shared<JobManager>(1);
    context = std::make_shared<ExecutionContext>(*job_manager, 0, 0);
    // create database
    const int flags = O_RDWR | O_DIRECT | O_CREAT | O_TRUNC;
    fd = open("test.db", flags, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (fd == -1)
        abort();
    db = std::make_shared<DB>(16ull * 1024ull * 1024ull * 1024ull, fd, true, job_manager->getWorkerCount());
}

void DBTestFixture::TearDown() {
    job_manager->stop();

    job_manager = nullptr;
    db = nullptr;

    close(fd);
}