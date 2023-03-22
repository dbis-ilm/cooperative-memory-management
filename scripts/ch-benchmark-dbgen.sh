#!/bin/sh
echo "Generating CH-benCHmark data with 100 warehouses..."
mkdir -p "${1}/100"
./chBenchmark -csv -wh 100 -pa "${1}/100"