from analysis import load_stats
import matplotlib.pyplot as plt
import os
import pandas as pd
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

def create_figures(result_path: str, figure_path: str):
    oltp_path = os.path.join(result_path, 'working-set')
    results_per_disk = {}
    for disk in os.listdir(oltp_path):
        disk_path = os.path.join(oltp_path, disk)
        results_per_disk[disk] = ([], [])
        for mem_limit in os.listdir(disk_path):
            df, config, _ = load_stats(os.path.join(disk_path, mem_limit))
            tpmc = df['no_success_count'].sum() / config.benchmark * 60 / 10**6
            if tpmc != 0:
                results_per_disk[disk][0].append(config.memory_limit // 1000**2)
                results_per_disk[disk][1].append(tpmc)

    fig, axs = plt.subplots(1, 1, squeeze=False)
    fig.set_size_inches(6, 2.6)

    # plot OLTP
    for disk, result in results_per_disk.items():
        df = pd.DataFrame({ 'x': result[0], 'y': result[1] }).sort_values(by='x')
        df.plot(ax=axs[0][0], x='x', y='y', label=disk, marker='x')
    axs[0][0].set_xlabel('Memory Limit [MB]')
    axs[0][0].set_ylabel('OLTP Throughput [MtpmC]')
    axs[0][0].grid()
    axs[0][0].set_title('TPC-C')
    axs[0][0].legend()

    fig.tight_layout()
    fig.savefig(os.path.join(figure_path, 'working_set.png'), dpi=300)