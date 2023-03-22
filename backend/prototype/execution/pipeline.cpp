#include "pipeline.hpp"

#include <algorithm>

#include "index_scan.hpp"
#include "join.hpp"
#include "operator.hpp"
#include "pipeline_starter.hpp"
#include "pipeline_breaker.hpp"
#include "qep.hpp"
#include "scan.hpp"
#include "sort.hpp"

void Pipeline::addOperator(const std::shared_ptr<OperatorBase>& op) {
    if (last_operator == nullptr) {
        auto op_as_starter = std::dynamic_pointer_cast<PipelineStarterBase>(op);
        op_as_starter->setPipeline(this);
        starter = op_as_starter;
    } else {
        last_operator->setNextOperator(op);
    }
    last_operator = op;
}

void Pipeline::addDependency(const size_t pipeline_id) {
    if (std::find(pipeline_dependencies.begin(), pipeline_dependencies.end(), pipeline_id) != pipeline_dependencies.end())
        throw std::runtime_error("Trying to add duplicate pipeline dependency!");
    pipeline_dependencies.push_back(pipeline_id);
}

void Pipeline::addBreaker(const std::shared_ptr<PipelineBreakerBase>& breaker) {
    addOperator(breaker);
    this->breaker = breaker;
}

std::shared_ptr<DefaultBreaker> Pipeline::addDefaultBreaker(const ExecutionContext context) {
    std::shared_ptr<DefaultBreaker> breaker = std::make_shared<DefaultBreaker>(current_columns, context.getWorkerCount());
    addBreaker(breaker);
    return breaker;
}

std::shared_ptr<JoinBreaker> Pipeline::addJoinBreaker(VMCache& vmcache, const ExecutionContext context) {
    BatchDescription output_desc;
    output_desc.addColumn(std::string("next_ptr"), std::make_shared<UnencodedTemporaryColumn<void*>>());
    for (auto& col : current_columns.getColumns())
        output_desc.addColumn(col.name, col.column);
    current_columns = BatchDescription(std::vector<NamedColumn>(output_desc.getColumns()));
    std::shared_ptr<JoinBreaker> breaker = std::make_shared<JoinBreaker>(vmcache, output_desc, context.getWorkerCount());
    addBreaker(breaker);
    return breaker;
}

std::shared_ptr<SortBreaker> Pipeline::addSortBreaker(const std::vector<NamedColumn>& sort_keys, const std::vector<Order>& sort_orders, size_t num_workers) {
    BatchDescription output_desc = BatchDescription(std::vector<NamedColumn>(current_columns.getColumns()));
    std::shared_ptr<SortBreaker> breaker = std::make_shared<SortBreaker>(output_desc, sort_keys, sort_orders, num_workers);
    addBreaker(breaker);
    return breaker;
}

std::shared_ptr<JoinProbe> Pipeline::addJoinProbe(VMCache& vmcache, const Pipeline& build_side, std::vector<NamedColumn>&& output_columns) {
    BatchDescription build_side_desc;
    BatchDescription probe_side_desc;
    // skip the 'next_ptr' column
    for (auto it = build_side.current_columns.getColumns().begin() + 1; it < build_side.current_columns.getColumns().end(); it++)
        build_side_desc.addColumn(it->name, it->column);
    for (auto& col : current_columns.getColumns())
        probe_side_desc.addColumn(col.name, col.column);
    BatchDescription output_desc(std::move(output_columns));
    current_columns = BatchDescription(std::vector<NamedColumn>(output_desc.getColumns()));
    auto join_build = std::dynamic_pointer_cast<JoinBuild>(build_side.breaker);
    if (join_build == nullptr)
        throw std::runtime_error("Pipeline without join build breaker supplied as build side in addJoinProbe()!");
    auto join_probe = std::make_shared<JoinProbe>(vmcache, join_build, build_side_desc, probe_side_desc, output_desc);
    addOperator(join_probe);
    addDependency(build_side.getId());
    return join_probe;
}

std::shared_ptr<SortOperator> Pipeline::addSort(VMCache& vmcache, const Pipeline& input) {
    auto sort_breaker = std::dynamic_pointer_cast<SortBreaker>(input.breaker);
    if (sort_breaker == nullptr)
        throw std::runtime_error("Pipeline without sort breaker supplied as input in addSort()!");
    // columns do not change compared to the input pipeline, as we only do sorting
    for (auto& col : input.current_columns.getColumns())
        current_columns.addColumn(col.name, col.column);
    auto sort = std::make_shared<SortOperator>(vmcache, sort_breaker);
    addOperator(sort);
    addDependency(input.getId());
    return sort;
}

ExecutablePipeline::ExecutablePipeline(size_t id, DB& db, const std::string& table_name, const std::vector<uint64_t>& scan_cids, const std::vector<NamedColumn>&& scan_columns, const ExecutionContext context) : Pipeline(id) {
    for (auto& col : scan_columns)
        current_columns.addColumn(col.name, col.column);
    addOperator(std::make_shared<ScanOperator>(db, table_name, scan_cids, std::forward<const std::vector<NamedColumn>>(scan_columns), context));
}

ExecutablePipeline::ExecutablePipeline(size_t id, DB& db, const std::string& table_name, Identifier search_value, const std::vector<uint64_t>& scan_cids, const std::vector<NamedColumn>&& scan_columns, const ExecutionContext context) : Pipeline(id) {
    for (auto& col : scan_columns)
        current_columns.addColumn(col.name, col.column);
    addOperator(std::make_shared<IndexScanOperator<1>>(db, table_name, CompositeKey<1> { search_value }, scan_cids, std::forward<const std::vector<NamedColumn>>(scan_columns), context));
}

ExecutablePipeline::ExecutablePipeline(size_t id, DB& db, const std::string& table_name, CompositeKey<2> search_value, const std::vector<uint64_t>& scan_cids, const std::vector<NamedColumn>&& scan_columns, const ExecutionContext context) : Pipeline(id) {
    for (auto& col : scan_columns)
        current_columns.addColumn(col.name, col.column);
    addOperator(std::make_shared<IndexScanOperator<2>>(db, table_name, search_value, scan_cids, std::forward<const std::vector<NamedColumn>>(scan_columns), context));
}

ExecutablePipeline::ExecutablePipeline(size_t id, DB& db, const std::string& table_name, CompositeKey<3> search_value, const std::vector<uint64_t>& scan_cids, const std::vector<NamedColumn>&& scan_columns, const ExecutionContext context) : Pipeline(id) {
    for (auto& col : scan_columns)
        current_columns.addColumn(col.name, col.column);
    addOperator(std::make_shared<IndexScanOperator<3>>(db, table_name, search_value, scan_cids, std::forward<const std::vector<NamedColumn>>(scan_columns), context));
}

ExecutablePipeline::ExecutablePipeline(size_t id, DB& db, const std::string& table_name, CompositeKey<4> search_value, const std::vector<uint64_t>& scan_cids, const std::vector<NamedColumn>&& scan_columns, const ExecutionContext context) : Pipeline(id) {
    for (auto& col : scan_columns)
        current_columns.addColumn(col.name, col.column);
    addOperator(std::make_shared<IndexScanOperator<4>>(db, table_name, search_value, scan_cids, std::forward<const std::vector<NamedColumn>>(scan_columns), context));
}

ExecutablePipeline::ExecutablePipeline(size_t id, DB& db, const std::string& table_name, CompositeKey<4> from_search_value, CompositeKey<4> to_search_value, const std::vector<uint64_t>& scan_cids, const std::vector<NamedColumn>&& scan_columns, const ExecutionContext context) : Pipeline(id) {
    for (auto& col : scan_columns)
        current_columns.addColumn(col.name, col.column);
    addOperator(std::make_shared<IndexScanOperator<4>>(db, table_name, from_search_value, to_search_value, scan_cids, std::forward<const std::vector<NamedColumn>>(scan_columns), context));
}

ExecutablePipeline::ExecutablePipeline(size_t id, DB& db, const std::string& table_name, const std::vector<uint64_t>& filter_column_cids, const std::vector<Identifier>&& filter_values, const std::vector<uint64_t>& scan_cids, const std::vector<NamedColumn>&& scan_columns, const ExecutionContext context) : Pipeline(id) {
    for (auto& col : scan_columns)
        current_columns.addColumn(col.name, col.column);
    addOperator(std::make_shared<FilteringScanOperator>(db, table_name, filter_column_cids, std::forward<const std::vector<Identifier>>(filter_values), scan_cids, std::forward<const std::vector<NamedColumn>>(scan_columns), context));
}

void ExecutablePipeline::startExecution(QEP* qep, const ExecutionContext context) {
    //std::cout << "Starting execution of pipeline " << id << std::endl;
    this->qep = qep;
    starter->pipelinePreExecutionSteps(context.getWorkerId());
    context.getDispatcher().scheduleJob(starter, context);
}