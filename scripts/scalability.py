from analysis import load_stats
from ExperimentConfiguration import ExperimentConfiguration
import matplotlib.pyplot as plt
import os
import pandas as pd
import parameters
import runner

def run_experiments(result_path: str):
    config = ExperimentConfiguration()
    config.benchmark = 60

    # OLTP scalability
    oltp_path = os.path.join(result_path, 'oltp-scalability')
    if not os.path.exists(oltp_path):
        os.mkdir(oltp_path)
    for num_threads in parameters.nums_threads:
        config.warmup = 30
        config.oltp = num_threads
        config.olap = 'none'
        config.parallel = 0
        try:
            runner.run_experiment(config, os.path.join(oltp_path, str(num_threads)))
        except:
            p = os.path.join(oltp_path, str(num_threads))
            print(f"Skipped '{p}'")

    # OLAP scalability
    olap_path = os.path.join(result_path, 'olap-scalability')
    if not os.path.exists(olap_path):
        os.mkdir(olap_path)
    for num_threads in parameters.nums_threads:
        config.warmup = 0
        config.oltp = 0
        config.parallel = num_threads
        config.olap = 'q09'
        config.olap_interval = 0
        config.olap_stdout = True
        try:
            runner.run_experiment(config, os.path.join(olap_path, str(num_threads)))
        except:
            p = os.path.join(olap_path, str(num_threads))
            print(f"Skipped '{p}'")

def create_figures(result_path: str, figure_path: str):
    oltp_path = os.path.join(result_path, 'oltp-scalability')
    oltp_threads = []
    tpmcs = []
    for num_threads in os.listdir(oltp_path):
        df, config, _ = load_stats(os.path.join(oltp_path, num_threads))
        tpmc = df['no_success_count'].sum() / config.benchmark * 60 / 10**6
        if tpmc != 0:
            oltp_threads.append(int(num_threads))
            tpmcs.append(tpmc)
    oltp_threads, tpmcs = zip(*sorted(zip(oltp_threads, tpmcs)))

    # load OLAP data
    olap_path = os.path.join(result_path, 'olap-scalability')
    olap_threads = []
    olap_throughput = []
    for num_threads in os.listdir(olap_path):
        df, config, _ = load_stats(os.path.join(olap_path, num_threads))
        llog_path = os.path.join(olap_path, num_threads, 'llog.csv')
        if not os.path.exists(llog_path):
            continue
        llog = pd.read_csv(llog_path)
        if not llog[llog.measurement == 'q09'].empty:
            # TODO: check variance!
            throughput = 10**6 / llog[llog.measurement == 'q09'].time.median() # 10**6 because llog is in us, and we want throughput per second
            olap_threads.append(int(num_threads))
            olap_throughput.append(throughput)
    olap_threads, olap_throughput = zip(*sorted(zip(olap_threads, olap_throughput)))

    fig, axs = plt.subplots(1, 2)
    fig.set_size_inches(6, 2.6)

    # plot OLTP
    axs[0].plot(oltp_threads, tpmcs, marker='x')
    axs[0].set_xlabel('# Threads')
    axs[0].set_ylabel('OLTP Throughput [MtpmC]')
    axs[0].grid()
    axs[0].set_title('TPC-C')

    # plot OLAP
    axs[1].plot(olap_threads, olap_throughput, marker='x')
    axs[1].set_xlabel('# Threads')
    axs[1].set_ylabel('OLAP Throughput [Q09/s]')
    axs[1].grid()
    axs[1].set_title('CH Q09')

    for ax in axs:
        ax.set_xticks([1, 8, 16, 38, 64, 76])

    fig.tight_layout()
    fig.savefig(os.path.join(figure_path, 'scalability.png'), dpi=300)