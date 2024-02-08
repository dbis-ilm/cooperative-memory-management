#include "db_test.hpp"

#include <random>
#include <sys/file.h>
#include <unistd.h>

#include "prototype/core/db.hpp"
#include "prototype/core/types.hpp"
#include "prototype/scheduling/execution_context.hpp"

void DBTestFixture::SetUp() {
    std::stringstream path_str;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dist(0, 4096);
    path_str << "test" << dist(gen) << ".db";
    path = path_str.str();
    lock_path = path + ".lock";
    lock_fd = open(lock_path.c_str(), O_CREAT);
    while (flock(lock_fd, LOCK_EX) != 0) {
        close(lock_fd);
        lock_fd = open(lock_path.c_str(), O_CREAT);
    }
    // delete pre-existing test database
    struct stat st;
    if (stat(path.c_str(), &st) == 0) {
        if (unlink(path.c_str()) != 0)
            throw std::runtime_error("Failed to delete existing test database");
    }
    uint64_t num_workers = JobManager::configureNumThreads(1);
    db = std::make_shared<DB>(16ull * 1024ull * 1024ull * 1024ull, path, false, false, false, false, num_workers, false);
    job_manager = std::make_shared<JobManager>(num_workers, *db);
    context = std::make_shared<ExecutionContext>(*job_manager, *db, 0, 0, false);
}

void DBTestFixture::TearDown() {
    job_manager->stop();

    job_manager = nullptr;
    db = nullptr;

    if (unlink(path.c_str()) != 0)
        throw std::runtime_error("Failed to delete test database");
    if (unlink(lock_path.c_str()) != 0)
        throw std::runtime_error("Failed to delete test lock file");
    flock(lock_fd, LOCK_UN);
    close(lock_fd);
}