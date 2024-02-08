import os
import parameters
import runner
from ExperimentConfiguration import ExperimentConfiguration

olap_workloads = ['simulated', 'q09', 'q09-continuous', 'q06', 'mixed', 'q06-continuous', 'mixed-continuous', 'none']
run_ablations = {
    'q09': 'all',
    'q09-continuous': [],
    'q06': [],
    'q06-continuous': [],
    'mixed': [],
    'mixed-continuous': [],
    'simulated': [],
    'none': ['-inmemory']
}
ablations = ['-inmemory', '+writeback', '+idlewriters', '+evictiontarget']

def run_trad_coop_experiment(basepath: str, mode: str, config: ExperimentConfiguration):
    result_path = os.path.join(basepath, mode)
    if mode == 'traditional':
        config.partitioning_strategy = 'partitioned'
        if config.olap == 'q06':
            config.partitioned_num_temp_pages = 2000
        elif config.olap == 'q09' or config.olap == 'mixed':
            config.partitioned_num_temp_pages = 170000
        else:
            # reserve temporary memory pages equal to half of the memory limit rounded up to the nearest multiple of 100 pages
            config.partitioned_num_temp_pages = (config.memory_limit // 2 // 4096 + 99) // 100 * 100
    elif mode == 'cooperative':
        config.partitioning_strategy = 'basic'
        config.partitioned_num_temp_pages = None
    else:
        raise RuntimeError(f"Unsupported mode '{mode}'")
    runner.run_experiment(config, result_path)


def run_experiments(result_path: str):
    config = ExperimentConfiguration()
    config.warmup = 30
    config.benchmark = 120
    config.oltp = parameters.oltp_threads
    config.parallel = parameters.physical_cores
    config.memory_limit = parameters.comparison_memory_limit
    basepath = os.path.join(result_path, 'trad-coop')
    if not os.path.exists(basepath):
        os.mkdir(basepath)
    for disk, database_path in parameters.disks.items():
        disk_path = os.path.join(basepath, disk)
        if not os.path.exists(disk_path):
            os.mkdir(disk_path)
        config.database_path = database_path
        for olap_workload in olap_workloads:
            workload_path = os.path.join(disk_path, olap_workload)
            if not os.path.exists(workload_path):
                os.mkdir(workload_path)
            if olap_workload == 'simulated':
                config.olap = 'simulated'
                config.olap_interval = 30
            elif olap_workload == 'q09':
                config.olap = 'q09'
                config.olap_interval = 30
            elif olap_workload == 'q09-continuous':
                config.olap = 'q09'
                config.olap_interval = 0
            elif olap_workload == 'q06':
                config.olap = 'q06'
                config.olap_interval = 30
            elif olap_workload == 'q06-continuous':
                config.olap = 'q06'
                config.olap_interval = 0
            elif olap_workload == 'mixed':
                config.olap = 'mixed'
                config.olap_interval = 30
            elif olap_workload == 'mixed-continuous':
                config.olap = 'mixed'
                config.olap_interval = 0
            elif olap_workload == 'none':
                config.olap = 'none'
                config.olap_interval = 0
            else:
                raise RuntimeError(f'Unknown OLAP workload {olap_workload}')
            # full prototype
            config.no_dirty_writeback = False
            config.no_async_flush = False
            config.no_eviction_target = False
            for mode in ['traditional', 'cooperative']:
                try:
                    run_trad_coop_experiment(workload_path, mode, config)
                except:
                    print(f"Skipped '{workload_path}'")
            # ablations
            for ablation in ablations:
                if not (run_ablations[olap_workload] == 'all' or ablation in run_ablations[olap_workload]):
                    continue
                ablation_path = os.path.join(workload_path, f'ablation{ablation}')
                if not os.path.exists(ablation_path):
                    os.mkdir(ablation_path)
                if ablation == '-inmemory':
                    config.no_dirty_writeback = False
                    config.no_async_flush = False
                    config.no_eviction_target = False
                    config.memory_limit = 64000000000
                elif ablation == '+writeback':
                    config.no_dirty_writeback = False
                    config.no_async_flush = True
                    config.no_eviction_target = True
                elif ablation == '+idlewriters':
                    config.no_dirty_writeback = False
                    config.no_async_flush = False
                    config.no_eviction_target = True
                elif ablation == '+evictiontarget':
                    config.no_dirty_writeback = False
                    config.no_async_flush = True
                    config.no_eviction_target = False
                else:
                    raise RuntimeError(f'Unknown ablation {ablation}')

                for mode in ['traditional', 'cooperative']:
                    try:
                        run_trad_coop_experiment(ablation_path, mode, config)
                    except:
                        print(f"Skipped '{ablation_path}'")
                config.memory_limit = parameters.comparison_memory_limit # reset memory limit for subsequent experiments after each ablation