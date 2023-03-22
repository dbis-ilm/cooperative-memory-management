#pragma once

#include <memory>
#include <stddef.h>

#include "../core/types.hpp"
#include "../scheduling/execution_context.hpp"
#include "batch.hpp"

class DB;
class DefaultBreaker;
class JoinBreaker;
class JoinProbe;
class OperatorBase;
enum class Order;
class PipelineStarterBase;
class PipelineBreakerBase;
class QEP;
class SortBreaker;
class SortOperator;
class VMCache;

class Pipeline {
protected:
    size_t id;
    std::shared_ptr<PipelineStarterBase> starter = nullptr;
    std::shared_ptr<PipelineBreakerBase> breaker = nullptr;
    std::shared_ptr<OperatorBase> last_operator = nullptr;
    QEP* qep = nullptr;
    std::vector<size_t> pipeline_dependencies; // ids of pipelines that have to execute before this pipeline can

public:
    BatchDescription current_columns; // TODO: make this private once we have generalized the scan implementation to support filtering

    Pipeline(size_t id) : id(id) {}
    size_t getId() const { return id; }

    void addDependency(const size_t pipeline_id);
    void addOperator(const std::shared_ptr<OperatorBase>& op);
    void addBreaker(const std::shared_ptr<PipelineBreakerBase>& breaker);

    std::shared_ptr<DefaultBreaker> addDefaultBreaker(const ExecutionContext context);
    std::shared_ptr<JoinBreaker> addJoinBreaker(VMCache& vmcache, const ExecutionContext context);
    std::shared_ptr<SortBreaker> addSortBreaker(const std::vector<NamedColumn>& sort_keys, const std::vector<Order>& sort_orders, size_t num_workers);
    std::shared_ptr<JoinProbe> addJoinProbe(VMCache& vmcache, const Pipeline& build_side, std::vector<NamedColumn>&& output_columns);
    std::shared_ptr<SortOperator> addSort(VMCache& vmcache, const Pipeline& input);

    QEP* getQEP() const { return qep; }
    const std::vector<size_t>& getDependencies() const { return pipeline_dependencies; }

    std::shared_ptr<PipelineBreakerBase> getBreaker() const { return breaker; }

    friend class JoinFactory;
};

class ExecutablePipeline: public Pipeline {
public:
    ExecutablePipeline(size_t id) : Pipeline(id) {}

    // executable pipeline that starts with a full table scan
    ExecutablePipeline(size_t id, DB& db, const std::string& table_name, const std::vector<uint64_t>& scan_cids, const std::vector<NamedColumn>&& scan_columns, const ExecutionContext context);
    // executable pipeline that starts with a index scan
    ExecutablePipeline(size_t id, DB& db, const std::string& table_name, Identifier search_value, const std::vector<uint64_t>& scan_cids, const std::vector<NamedColumn>&& scan_columns, const ExecutionContext context);
    ExecutablePipeline(size_t id, DB& db, const std::string& table_name, CompositeKey<2> search_value, const std::vector<uint64_t>& scan_cids, const std::vector<NamedColumn>&& scan_columns, const ExecutionContext context);
    ExecutablePipeline(size_t id, DB& db, const std::string& table_name, CompositeKey<3> search_value, const std::vector<uint64_t>& scan_cids, const std::vector<NamedColumn>&& scan_columns, const ExecutionContext context);
    ExecutablePipeline(size_t id, DB& db, const std::string& table_name, CompositeKey<4> search_value, const std::vector<uint64_t>& scan_cids, const std::vector<NamedColumn>&& scan_columns, const ExecutionContext context);
    ExecutablePipeline(size_t id, DB& db, const std::string& table_name, CompositeKey<4> from_search_value, CompositeKey<4> to_search_value, const std::vector<uint64_t>& scan_cids, const std::vector<NamedColumn>&& scan_columns, const ExecutionContext context);
    // executable pipeline that starts with a full table scan with filtering
    ExecutablePipeline(size_t id, DB& db, const std::string& table_name, const std::vector<uint64_t>& filter_column_cids, const std::vector<uint32_t>&& filter_values, const std::vector<uint64_t>& scan_cids, const std::vector<NamedColumn>&& scan_columns, const ExecutionContext context);

    void startExecution(QEP* qep, const ExecutionContext context);
};