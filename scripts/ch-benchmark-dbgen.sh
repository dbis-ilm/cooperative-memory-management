#!/bin/sh
echo "Generating CH-benCHmark data with ${2} warehouses..."
mkdir -p "${1}/${2}"
./chBenchmark -csv -wh ${2} -pa "${1}/${2}"