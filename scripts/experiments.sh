#!/bin/bash

SATA_BASEDIR=/data
NVME_BASEDIR=/data2
NUM_WAREHOUSES=100
CH_DATA_BASEDIR=data/tpcch/${NUM_WAREHOUSES}
RESULT_BASEDIR=results
NUMACTL="numactl -c 0"

WARMUP_DURATION=60
BENCHMARK_DURATION=120

# benchmark a simulated and a real (CH-benCHmark Q09) OLAP query
declare -A OLAP_QUERY_ARGS
OLAP_QUERY_ARGS[sim_olap]=""
OLAP_QUERY_ARGS[real_olap]="--real_olap"

# benchmark with the database stored on a SATA or NVMe SSD
declare -A DISKS
DISKS[sata]=${SATA_BASEDIR}
DISKS[nvme]=${NVME_BASEDIR}

# import database
mkdir -p ${RESULT_BASEDIR}
(set -x; ${NUMACTL} build/frontend/tpcch ${NVME_BASEDIR}/tpcch-${NUM_WAREHOUSES}.db --ch_path=${CH_DATA_BASEDIR} --import_only |& tee ${RESULT_BASEDIR}/import.out && cp ${NVME_BASEDIR}/tpcch-${NUM_WAREHOUSES}.db ${SATA_BASEDIR}/tpcch-${NUM_WAREHOUSES}.db)

# run benchmarks
for RESULT_SUBDIR in "${!OLAP_QUERY_ARGS[@]}"; do
    ADDITIONAL_ARGS=${OLAP_QUERY_ARGS[$RESULT_SUBDIR]}
    RESULT_DIR="${RESULT_BASEDIR}/${RESULT_SUBDIR}"
    mkdir -p ${RESULT_DIR}

    for DISK in "${!DISKS[@]}"; do
        DISK_BASEDIR=${DISKS[$DISK]}
        # cooperative memory management
        (set -x; ${NUMACTL} build/frontend/tpcch ${DISK_BASEDIR}/tpcch-${NUM_WAREHOUSES}.db --collect_stats=${RESULT_DIR}/stats_coop_${DISK}.csv --latency_log=${RESULT_DIR}/latencies_coop_${DISK}.csv --memory_limit=2000000000 --warmup=${WARMUP_DURATION} --benchmark=${BENCHMARK_DURATION} ${ADDITIONAL_ARGS} |& tee ${RESULT_DIR}/coop_${DISK}.out)
        # traditional memory management
        (set -x; ${NUMACTL} build/frontend/tpcch ${DISK_BASEDIR}/tpcch-${NUM_WAREHOUSES}.db --collect_stats=${RESULT_DIR}/stats_trad_${DISK}.csv --latency_log=${RESULT_DIR}/latencies_trad_${DISK}.csv --memory_limit=2000000000 --partitioning_strategy=partitioned --partitioned_num_temp_pages=244200 --warmup=${WARMUP_DURATION} --benchmark=${BENCHMARK_DURATION} ${ADDITIONAL_ARGS} |& tee ${RESULT_DIR}/trad_${DISK}.out)
    done
done