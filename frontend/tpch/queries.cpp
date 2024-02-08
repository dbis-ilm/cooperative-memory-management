#include "queries.hpp"

#include "prototype/core/db.hpp"
#include "prototype/core/types.hpp"
#include "prototype/execution/join.hpp"
#include "prototype/execution/table_column.hpp"
#include "prototype/execution/temporary_column.hpp"
#include "prototype/execution/qep.hpp"
#include "execution/q06_agg.hpp"
#include "execution/q06_scan.hpp"
#include "execution/q09_agg.hpp"
#include "execution/q09_scan.hpp"
#include "schema.hpp"

// column definitions
NamedColumn n_nationkey = NamedColumn(std::string("N_NATIONKEY"), std::make_shared<UnencodedTableColumn<Identifier>>(N_NATIONKEY_CID));
NamedColumn n_regionkey = NamedColumn(std::string("N_REGIONKEY"), std::make_shared<UnencodedTableColumn<Identifier>>(N_REGIONKEY_CID));

NamedColumn ps_partkey = NamedColumn(std::string("PS_PARTKEY"), std::make_shared<UnencodedTableColumn<Identifier>>(PS_PARTKEY_CID));
NamedColumn ps_suppkey = NamedColumn(std::string("PS_SUPPKEY"), std::make_shared<UnencodedTableColumn<Identifier>>(PS_SUPPKEY_CID));
NamedColumn ps_availqty = NamedColumn(std::string("PS_AVAILQTY"), std::make_shared<UnencodedTableColumn<Integer>>(PS_AVAILQTY_CID));
NamedColumn ps_supplycost = NamedColumn(std::string("PS_SUPPLYCOST"), std::make_shared<UnencodedTableColumn<Decimal<2>>>(PS_SUPPLYCOST_CID));

NamedColumn s_nationkey = NamedColumn(std::string("S_NATIONKEY"), std::make_shared<UnencodedTableColumn<Identifier>>(S_NATIONKEY_CID));
NamedColumn s_suppkey = NamedColumn(std::string("S_SUPPKEY"), std::make_shared<UnencodedTableColumn<Identifier>>(S_SUPPKEY_CID));

NamedColumn p_partkey = NamedColumn(std::string("P_PARTKEY"), std::make_shared<UnencodedTableColumn<Identifier>>(P_PARTKEY_CID));
NamedColumn p_size = NamedColumn(std::string("P_SIZE"), std::make_shared<UnencodedTableColumn<Integer>>(P_SIZE_CID));

NamedColumn l_suppkey = NamedColumn(std::string("L_SUPPKEY"), std::make_shared<UnencodedTableColumn<Identifier>>(L_SUPPKEY_CID));
NamedColumn l_partkey = NamedColumn(std::string("L_PARTKEY"), std::make_shared<UnencodedTableColumn<Identifier>>(L_PARTKEY_CID));
NamedColumn l_orderkey = NamedColumn(std::string("L_ORDERKEY"), std::make_shared<UnencodedTableColumn<Identifier>>(L_ORDERKEY_CID));
NamedColumn l_extendedprice = NamedColumn(std::string("L_EXTENDEDPRICE"), std::make_shared<UnencodedTableColumn<Decimal<2>>>(L_EXTENDEDPRICE_CID));
NamedColumn l_discount = NamedColumn(std::string("L_DISCOUNT"), std::make_shared<UnencodedTableColumn<Decimal<2>>>(L_DISCOUNT_CID));
NamedColumn l_quantity = NamedColumn(std::string("L_QUANTITY"), std::make_shared<UnencodedTableColumn<Decimal<2>>>(L_QUANTITY_CID));
NamedColumn l_shipdate = NamedColumn(std::string("L_SHIPDATE"), std::make_shared<UnencodedTableColumn<Date>>(L_SHIPDATE_CID));

NamedColumn o_orderkey = NamedColumn(std::string("O_ORDERKEY"), std::make_shared<UnencodedTableColumn<Identifier>>(O_ORDERKEY_CID));

// queries
std::shared_ptr<QEP> getQEP(DB& db, const std::string& query_name, const ExecutionContext context) {
    if (query_name == "scan_nation") {
        std::vector<std::unique_ptr<ExecutablePipeline>> pipelines;
        pipelines.push_back(std::make_unique<ExecutablePipeline>(
            0, db, "NATION", std::vector<NamedColumn>({
                n_nationkey,
                n_regionkey
            }),
            context)
        );
        pipelines[0]->addDefaultBreaker(context);
        return std::make_shared<QEP>(std::move(pipelines));
    }
    if (query_name == "scan_lineitem") {
        std::vector<std::unique_ptr<ExecutablePipeline>> pipelines;
        pipelines.push_back(std::make_unique<ExecutablePipeline>(
            0, db, "LINEITEM", std::vector<NamedColumn>({
                l_suppkey,
                l_partkey,
                l_orderkey,
                l_extendedprice,
                l_discount,
                l_quantity
            }),
            context)
        );
        pipelines[0]->addDefaultBreaker(context);
        return std::make_shared<QEP>(std::move(pipelines));
    }
    if (query_name == "scan_partsupp") {
        std::vector<std::unique_ptr<ExecutablePipeline>> pipelines;
        pipelines.push_back(std::make_unique<ExecutablePipeline>(
            0, db, "PARTSUPP", std::vector<NamedColumn>({
                ps_partkey,
                ps_suppkey,
                ps_availqty,
                ps_supplycost
            }),
            context)
        );
        pipelines[0]->addDefaultBreaker(context);
        return std::make_shared<QEP>(std::move(pipelines));
    }
    if (query_name == "q06") {
        std::vector<std::unique_ptr<ExecutablePipeline>> pipelines;
        pipelines.push_back(std::make_unique<ExecutablePipeline>(0));
        pipelines[0]->addOperator(std::make_shared<Q06ScanOperator>(db, context));
        BatchDescription output_desc = BatchDescription(std::vector<NamedColumn>({ NamedColumn(std::string("revenue"), std::make_shared<UnencodedTemporaryColumn<Decimal<4>>>()) }));
        pipelines[0]->addBreaker(std::make_shared<Q06AggregationOperator>(db, output_desc));
        return std::make_shared<QEP>(std::move(pipelines));
    }
    if (query_name == "q09_mod" || query_name == "q09_mod_no_sel") {
        /*
            select
                sum(l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity) as sum_profit
            from
                part,
                supplier,
                lineitem,
                partsupp,
                orders,
                nation
            where
                s_suppkey = l_suppkey
                and ps_suppkey = l_suppkey
                and ps_partkey = l_partkey
                and p_partkey = l_partkey
                and o_orderkey = l_orderkey
                and s_nationkey = n_nationkey
                and p_size < 5
        */

        std::vector<std::unique_ptr<ExecutablePipeline>> pipelines;
        // (0) scan NATION into JoinBreaker for SUPPLIER join
        pipelines.push_back(std::make_unique<ExecutablePipeline>(pipelines.size(), db, "NATION", std::vector<NamedColumn>({ n_nationkey }), context));
        pipelines.back()->addJoinBreaker(db.vmcache, context);

        // (1) + (2) hash build for (0)
        JoinFactory::createBuildPipelines(pipelines, db.vmcache, *pipelines.back(), n_nationkey.column->getValueTypeSize());

        // (3) scan SUPPLIER, join on (2), into JoinBreaker for PART/PARTSUPP join
        pipelines.push_back(std::make_unique<ExecutablePipeline>(pipelines.size(), db, "SUPPLIER", std::vector<NamedColumn>({ s_nationkey, s_suppkey }), context));
        pipelines.back()->addJoinProbe(db.vmcache, *pipelines[2], std::vector<NamedColumn>({ s_suppkey }));
        pipelines.back()->addJoinBreaker(db.vmcache, context);

        // (4) + (5) hash build for (3)
        JoinFactory::createBuildPipelines(pipelines, db.vmcache, *pipelines.back(), s_suppkey.column->getValueTypeSize());

        // (6) scan PART (with P_SIZE < 5 filter) into JoinBreaker
        // TODO: simplify this using ExecutablePipeline's specialised scan constructor once the general-purpose scan operator can do filtering
        if (query_name == "q09_mod") {
            pipelines.push_back(std::make_unique<ExecutablePipeline>(pipelines.size()));
            pipelines.back()->addOperator(std::make_shared<Q09PartScanOperator>(db, std::vector<NamedColumn>({ p_partkey }), context));
            pipelines.back()->current_columns.addColumn(p_partkey.name, p_partkey.column);
        } else {
            pipelines.push_back(std::make_unique<ExecutablePipeline>(pipelines.size(), db, "PART", std::vector<NamedColumn>({ p_partkey }), context));
        }
        pipelines.back()->addJoinBreaker(db.vmcache, context);

        // (7) + (8) hash build for (6)
        JoinFactory::createBuildPipelines(pipelines, db.vmcache, *pipelines.back(), p_partkey.column->getValueTypeSize());

        // (9) scan PARTSUPP, join on (8), join on (5), into JoinBreaker for LINEITEM join
        pipelines.push_back(std::make_unique<ExecutablePipeline>(pipelines.size(), db, "PARTSUPP", std::vector<NamedColumn>({ ps_partkey, ps_suppkey, ps_supplycost }), context));
        pipelines.back()->addJoinProbe(db.vmcache, *pipelines[8], std::vector<NamedColumn>({ ps_suppkey, p_partkey, ps_supplycost }));
        pipelines.back()->addJoinProbe(db.vmcache, *pipelines[5], std::vector<NamedColumn>({ s_suppkey, p_partkey, ps_supplycost }));
        pipelines.back()->addJoinBreaker(db.vmcache, context);

        // (10) + (11) hash build for (9)
        JoinFactory::createBuildPipelines(pipelines, db.vmcache, *pipelines.back(), s_suppkey.column->getValueTypeSize() + p_partkey.column->getValueTypeSize());

        // (12) scan LINEITEM, join on (11), into JoinBreaker for ORDERS join
        pipelines.push_back(std::make_unique<ExecutablePipeline>(
            pipelines.size(), db, "LINEITEM",
            std::vector<NamedColumn>({
                l_suppkey,
                l_partkey,
                l_orderkey,
                l_extendedprice,
                l_discount,
                l_quantity
            }),
            context
        ));
        pipelines.back()->addJoinProbe(db.vmcache, *pipelines[11], std::vector<NamedColumn>({ l_orderkey, l_extendedprice, l_discount, l_quantity, ps_supplycost }));
        pipelines.back()->addJoinBreaker(db.vmcache, context);

        // (13) + (14) hash build for (12)
        JoinFactory::createBuildPipelines(pipelines, db.vmcache, *pipelines.back(), l_orderkey.column->getValueTypeSize());

        // (15) scan ORDERS, join on (14), aggregate SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT) - PS_SUPPLYCOST * L_QUANTITY)
        pipelines.push_back(std::make_unique<ExecutablePipeline>(
            pipelines.size(), db, "ORDERS",
            std::vector<NamedColumn>({ o_orderkey }),
            context
        ));
        pipelines.back()->addJoinProbe(db.vmcache, *pipelines[14], std::vector<NamedColumn>({ l_extendedprice, l_discount, ps_supplycost, l_quantity }));
        BatchDescription output_desc15 = BatchDescription(std::vector<NamedColumn>({
            NamedColumn(std::string("sum_profit"), std::make_shared<UnencodedTemporaryColumn<Decimal<4>>>())
        }));
        pipelines.back()->addBreaker(std::make_shared<Q09AggregationOperator>(db, output_desc15));

        return std::make_shared<QEP>(std::move(pipelines));
    }

    return nullptr;
}