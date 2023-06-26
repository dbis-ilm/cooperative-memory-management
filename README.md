# Cooperative Memory Management for Table and Temporary Data

This is a prototype system for evaluating cooperative memory management for database management systems.
See the [paper](paper/p9-lasch.pdf) for a discussion of the idea and experimental results:

Robert Lasch, Thomas Legler, Norman May, Bernhard Scheirle, Kai-Uwe Sattler: *Cooperative Memory Management for Table and Temporary Data*, 1st Workshop on Simplicity in Management of Data (SiMoD '23), June 23, 2023, Bellevue, WA, USA.

## Running the experiments
Requirements for building the project are:
* unixODBC (required to build the CH-benCHmark binaries used for dataset generation)

Build the project as follows:
```
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release -DVTUNE_PROFILING=FALSE -DCOLLECT_CACHE_TRACES=FALSE ..
cmake --build .
```

The build process includes generating the CH-benCHmark data that is used in the experiments.

Before running the experiments, you may have to adjust the following variables in ``scripts/experiments.sh``:
Variable        | Description
--------------- | -----------
SATA_BASEDIR    | should point to a directory backed by a SATA SSD; the database file for the SATA SSD experiments will be saved here
NVME_BASEDIR    | should point to a directory backed by an NVMe SSD; the database file for the NVMe SSD experiments will be saved here

After having built the binaries and configuring ``experiments.sh`` properly, you can run the experiments from the base directory as follows:
```
scripts/experiments.sh
```

Note: The default experiment configuration runs 64 concurrent OLTP streams, which requires at least 66 CPU threads to be available.
If you want to run the experiments on a machine with fewer available threads, you can adjust the number of OLTP streams by modifying ``scripts/experiments.sh`` to additionally pass ``--oltp=<number of streams>`` to the ``tpcch`` benchmark runner invocations.

## License

MIT
