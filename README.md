# Cooperative Memory Management for Table and Temporary Data

This is a prototype system for evaluating cooperative memory management for database management systems and comparing it to traditional memory management. The system and experimental results are presented in our paper submitted (**currently under review**) to the 28th International Conference on Extending Database Technology (EDBT '25):

Robert Lasch, Thomas Legler, Norman May, Bernhard Scheirle, Kai-Uwe Sattler: *Cooperative Memory Management for Cost-Efficient HTAP*

An initial discussion of the idea and preliminary experimental results can also be found in our [paper](https://doi.org/10.1145/3596225.3596230) published in the 1st Workshop on Simplicity in Management of Data (SiMoD):

Robert Lasch, Thomas Legler, Norman May, Bernhard Scheirle, Kai-Uwe Sattler: *Cooperative Memory Management for Table and Temporary Data*

The earlier version of the prototype used to obtain the results presented there can be found in the ``simod`` branch.

## Running the experiments
Make sure to initialize git submodules first: ``git submodule init && git submodule update``

Requirements for building the project and running the experiments are:
* ``cmake``
* unixODBC (required to build the CH-benCHmark binaries used for dataset generation)
* ``numactl`` and libnuma-dev(el) (for ``<numa.h>``)

Build the project as follows:
```
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release -DVTUNE_PROFILING=FALSE -DCOLLECT_CACHE_TRACES=FALSE ..
cmake --build .
```

The build process includes generating the CH-benCHmark data that is used in the experiments.

Before running the experiments, you may want to adjust the following variables in ``scripts/parameters.py``:
Variable        | Description
--------------- | -----------
nums_threads    | the numbers of threads to run the scalability experiment with
oltp_threads    | the number of OLTP threads to use in each experiment except for scalability
disks           | should be a dictionary of disk names and paths to where the test database file should be put for each disk to run benchmarks with

After having built the binaries and configuring ``parameters.py`` properly, you can run the experiments from the base directory as follows:
```
python3 scripts/run_experiments.py <result directory>
```

## License

MIT
