import os
import parameters
import runner
from ExperimentConfiguration import ExperimentConfiguration

def run_experiments(result_path: str):
    config = ExperimentConfiguration()
    config.warmup = 30
    config.benchmark = 30
    config.oltp = parameters.oltp_threads
    config.parallel = parameters.physical_cores
    config.olap = 'none'
    basepath = os.path.join(result_path, 'working-set')
    if not os.path.exists(basepath):
        os.mkdir(basepath)
    for disk, database_path in parameters.disks.items():
        disk_path = os.path.join(basepath, disk)
        if not os.path.exists(disk_path):
            os.mkdir(disk_path)
        config.database_path = database_path
        for memory_limit_mb in [500, 1000, 1500, 2000, 2500, 3000, 3500, 4000]:
            config.memory_limit = memory_limit_mb * 1000**2
            experiment_path = os.path.join(disk_path, f'{memory_limit_mb}M')
            try:
                runner.run_experiment(config, experiment_path)
            except:
                print(f"Skipped '{experiment_path}'")
