#pragma once

#include <memory>
#include <string>

class DB;
class ExecutionContext;
class QEP;

std::shared_ptr<QEP> getQEP(DB& db, const std::string& query_name, const ExecutionContext context);