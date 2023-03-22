#pragma once

#include <chrono>
#include <iostream>
#include <map>
#include <string>
#include <sys/stat.h>
#include <vector>

#include "../utils/CSV.hpp"
#include "../core/db.hpp"
#include "pipeline.hpp"
#include "pipeline_breaker.hpp"

struct CSVColumnSpec {
    ParseTypeDescription type;
    PageId destination_column_basepage;

    CSVColumnSpec(ParseTypeDescription type, PageId destination_column_basepage)
    : type(type)
    , destination_column_basepage(destination_column_basepage) {}
};

class CSVImportOperator : public PipelineStarterBreakerBase {
private:
    DB& db;
    std::string path;
    size_t size;
    char sep;
    std::chrono::time_point<std::chrono::system_clock> begin;
    std::unordered_map<size_t, CSVColumnSpec> columns;
    size_t num_columns;
    std::vector<ParseTypeDescription> types;
    size_t& row_count_dest;

public:
    CSVImportOperator(BatchDescription& batch_description, DB& db, const std::string& path, char sep, std::unordered_map<size_t, CSVColumnSpec> columns, size_t num_columns, size_t& row_count_dest)
    : PipelineStarterBreakerBase(batch_description)
    , db(db)
    , path(path)
    , sep(sep)
    , columns(columns)
    , num_columns(num_columns)
    , row_count_dest(row_count_dest) {
        struct stat st;
        if (stat(path.c_str(), &st) != 0) {
            std::cout << "Error: Could not stat " << path << std::endl;
            throw std::runtime_error("Could not stat csv file");
        }
        size = st.st_size;

        for (size_t i = 0; i < num_columns; i++) {
            auto iter = columns.find(i);
            if (iter == columns.end()) {
                types.push_back(ParseTypeDescription::Skip());
            } else {
                types.push_back(iter->second.type);
            }
        }
    }

    void execute(size_t from, size_t to, uint32_t worker_id) override {
        if (from == 0)
            begin = std::chrono::system_clock::now();

        std::fstream csv;
        csv.open(path.c_str(), std::ios::in | std::ios::binary);
        assert(csv.is_open());

        // thread-local storage for columns parsed from the CSV, copied into the actual database after parsing
        std::vector<void*> local_destinations;
        for (size_t i = 0; i < num_columns; i++) {
            auto iter = columns.find(i);
            if (iter == columns.end()) {
                local_destinations.push_back(nullptr);
            } else {
                switch (iter->second.type.type) {
                    case ParseType::Int32:
                        local_destinations.push_back(new std::vector<uint32_t>());
                        break;
                    case ParseType::Date:
                        local_destinations.push_back(new std::vector<uint32_t>());
                        break;
                    case ParseType::DateTime:
                        local_destinations.push_back(new std::vector<uint64_t>());
                        break;
                    case ParseType::Decimal:
                        local_destinations.push_back(new std::vector<int64_t>());
                        break;
                    case ParseType::Char:
                        local_destinations.push_back(new std::vector<char>());
                        break;
                    default:
                        throw std::runtime_error("Unsupported type in CSVColumnSpec");
                }
            }
        }
        const size_t parsed_rows = parse_csv_chunk(csv, from, to - from, sep, types, local_destinations);

        // copy parsed rows to main db
        {
            std::lock_guard<std::mutex> guard(db.write_lock);
            for (size_t i = 0; i < num_columns; i++) {
                auto iter = columns.find(i);
                if (iter != columns.end()) {
                    switch (iter->second.type.type) {
                        case ParseType::Int32: {
                                std::vector<uint32_t>* vec = reinterpret_cast<std::vector<uint32_t>*>(local_destinations[i]);
                                db.appendFixedSizeValues(row_count_dest, iter->second.destination_column_basepage, vec->data(), sizeof(uint32_t), vec->size(), worker_id);
                                break;
                            }
                        case ParseType::Date: {
                                std::vector<uint32_t>* vec = reinterpret_cast<std::vector<uint32_t>*>(local_destinations[i]);
                                db.appendFixedSizeValues(row_count_dest, iter->second.destination_column_basepage, vec->data(), sizeof(uint32_t), vec->size(), worker_id);
                                break;
                            }
                        case ParseType::DateTime: {
                                std::vector<uint64_t>* vec = reinterpret_cast<std::vector<uint64_t>*>(local_destinations[i]);
                                db.appendFixedSizeValues(row_count_dest, iter->second.destination_column_basepage, vec->data(), sizeof(uint64_t), vec->size(), worker_id);
                                break;
                            }
                        case ParseType::Decimal: {
                                std::vector<int64_t>* vec = reinterpret_cast<std::vector<int64_t>*>(local_destinations[i]);
                                db.appendFixedSizeValues(row_count_dest, iter->second.destination_column_basepage, vec->data(), sizeof(int64_t), vec->size(), worker_id);
                                break;
                            }
                        case ParseType::Char: {
                                std::vector<char>* vec = reinterpret_cast<std::vector<char>*>(local_destinations[i]);
                                const size_t str_len = iter->second.type.params.len;
                                db.appendFixedSizeValues(row_count_dest, iter->second.destination_column_basepage, vec->data(), str_len, vec->size() / str_len, worker_id);
                                break;
                            }
                        default:
                            throw std::runtime_error("Unsupported type in CSVColumnSpec");
                    }
                }
            }
            row_count_dest += parsed_rows;
        }
        csv.close();
        for (size_t i = 0; i < num_columns; i++) {
            auto iter = columns.find(i);
            if (iter != columns.end()) {
                switch (iter->second.type.type) {
                    case ParseType::Int32: {
                            delete reinterpret_cast<std::vector<uint32_t>*>(local_destinations[i]);
                            break;
                        }
                    case ParseType::Date: {
                            delete reinterpret_cast<std::vector<uint32_t>*>(local_destinations[i]);
                            break;
                        }
                    case ParseType::DateTime: {
                            delete reinterpret_cast<std::vector<uint64_t>*>(local_destinations[i]);
                            break;
                        }
                    case ParseType::Decimal: {
                            delete reinterpret_cast<std::vector<int64_t>*>(local_destinations[i]);
                            break;
                        }
                    case ParseType::Char: {
                            delete reinterpret_cast<std::vector<char>*>(local_destinations[i]);
                            break;
                        }
                    default:
                        throw std::runtime_error("Unsupported type in CSVColumnSpec");
                }
            }
        }
    }

    size_t getInputSize() const override { return size; }
    size_t getMorselSizeHint() const override { return 1024 * 1024; }

    // this operator produces no result
    void consumeBatches(std::vector<std::shared_ptr<Batch>>&, uint32_t) override { }
};

class CSVImportPipeline : public ExecutablePipeline {

public:
    CSVImportPipeline(size_t id, DB& db, const std::string& path, char sep, std::unordered_map<size_t, CSVColumnSpec> columns, size_t num_columns, size_t& row_count_dest)
    : ExecutablePipeline(id) {
        BatchDescription desc;
        addBreaker(std::make_shared<CSVImportOperator>(desc, db, path, sep, columns, num_columns, row_count_dest));
    }
};