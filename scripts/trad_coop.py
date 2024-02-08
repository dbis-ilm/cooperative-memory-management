from analysis import load_latencies, load_stats
from matplotlib.lines import Line2D
from matplotlib.patches import Ellipse
import matplotlib.pyplot as plt
import numpy as np
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

def create_figures(result_path: str, figure_path: str):
    #####################
    # comparison figure #
    #####################
    DISK = 'NVMe'
    OLAP_CONFIGS = ['simulated', 'q09']
    dfs = {}
    memory_limits = {}
    overlays = {}
    benchmark_time = 0
    for olap in OLAP_CONFIGS:
        for paradigm in ['traditional', 'cooperative']:
            file = os.path.join(result_path, 'trad-coop', DISK, olap, paradigm, 'stats.csv')
            name = os.path.dirname(file[len(result_path) + 1:]).replace('/', '-')
            df, config, overlay_cols = load_stats(os.path.dirname(file))
            dfs[name] = df
            memory_limits[name] = config.memory_limit / 1000**2
            overlays[name] = overlay_cols
            benchmark_time = max(benchmark_time, config.benchmark)

    def pad_right(x, y, end_x):
        if len(x) > 0 and len(y) > 0 and x[-1] < end_x:
            x = np.append(x, [end_x])
            y = np.append(y, [y[-1]])
        return x, y

    fig, axs = plt.subplots(len(OLAP_CONFIGS), 2, squeeze=False)
    c_sub = 0
    for c, (name, df) in enumerate(dfs.items()):
        stackplot_cols = ['temp_pages', 'data_pages']
        stackplot_labels = ['Reserved temporary memory', 'Buffer pool memory']
        stackplot_colors = [parameters.plot_colors['temporary'], parameters.plot_colors['bufferpool']]
        if 'dirty_data_pages' in df.columns:
            df.data_pages -= df.dirty_data_pages
            stackplot_cols.append('dirty_data_pages')
            stackplot_labels.append('Buffer pool memory (dirty)')
            stackplot_colors.append(parameters.plot_colors['bufferpool_dirty'])
        ax = axs[(c - c_sub) // 2][(c - c_sub) % 2]
        ax.stackplot(df['elapsed'], df[stackplot_cols].T, labels=stackplot_labels, colors=stackplot_colors)
        ax.stackplot(df['elapsed'], df[['temp_in_use']].T, labels=['Used temporary memory'], colors=[parameters.plot_colors['temporary_in_use']])
        # throughput plot prerequisites
        overlay_col = 'no_success_count'
        THROUGHPUT_WINDOW = 0.5 # compute average throughput for 'THROUGHPUT_WINDOW' second windows
        overlay_x = np.arange(df['elapsed'].min(), df['elapsed'].max() - THROUGHPUT_WINDOW, THROUGHPUT_WINDOW)
        overlay_y = []
        ax2 = ax.twinx()
        ax2.set_ylabel('Throughput [MtpmC]')  # we already handled the x-label with ax1
        # for 'cooperative' plots, plot 'traditional' throughput in the background for comparison, and vice-versa
        OTHER_THROUGHPUT_WINDOW = THROUGHPUT_WINDOW
        other_name = name.replace('cooperative', 'traditional') if 'cooperative' in name else name.replace('traditional', 'cooperative')
        other_overlay_x = np.arange(df['elapsed'].min(), df['elapsed'].max() - OTHER_THROUGHPUT_WINDOW, OTHER_THROUGHPUT_WINDOW)
        other_overlay_y = []
        for start in other_overlay_x:
            other_overlay_y.append(dfs[other_name][dfs[other_name]['elapsed'].between(start, start + OTHER_THROUGHPUT_WINDOW)][overlay_col].sum() * (60 / OTHER_THROUGHPUT_WINDOW) / 10**6)
        other_overlay_x, other_overlay_y = pad_right(other_overlay_x, other_overlay_y, benchmark_time)
        ax2.plot(other_overlay_x, other_overlay_y, color=parameters.plot_colors[overlay_col], alpha=0.3)
        # plot transaction throughput
        for start in overlay_x:
            overlay_y.append(df[df['elapsed'].between(start, start + THROUGHPUT_WINDOW)][overlay_col].sum() * (60 / THROUGHPUT_WINDOW) / 10**6)
        overlay_x, overlay_y = pad_right(overlay_x, overlay_y, benchmark_time)
        ax2.plot(overlay_x, overlay_y, color=parameters.plot_colors[overlay_col])
        ax2.tick_params(axis='y')
        ax2.set_ylim((0, 1.5))
        ax.set_xlabel('Elapsed time [s]')
        ax.set_ylabel('Data Volume [MB]')
        if c == 0:
            ax.set_title('Traditional')
        elif c == 1:
            ax.set_title('Cooperative')
        ax.set_xlim(0, benchmark_time)
        ax.set_ylim(0, 2000)

    handles, labels = axs[0][0].get_legend_handles_labels()
    handles.append(Line2D([], [], color=parameters.plot_colors['no_success_count'], linewidth=1.5))
    labels.append('OLTP Throughput')
    fig.set_size_inches(w=12, h=2.2 * len(OLAP_CONFIGS))
    fig.legend(handles, labels, ncol=5, loc='lower center')
    fig.tight_layout(rect=(0.0, 0.1, 1.0, 1.0))

    fig.savefig(os.path.join(figure_path, 'comparison.png'), dpi=300)

    ####################
    # latencies figure #
    ####################
    ablation_labels = {
        '-inmemory': 'in-memory',
        '+writeback': 'naïve writes',
        '+idlewriters': '+ idle flush',
        '+evictiontarget': '+ target mechanism',
        'full': '+ both',
    }
    ablations = {}

    path = os.path.join(result_path, 'trad-coop/NVMe/q09')
    for ablation in ['ablation-inmemory', 'ablation+writeback', 'ablation+idlewriters', 'ablation+evictiontarget'] + ['']:
        basepath = os.path.join(path, ablation)
        name = ablation_labels['full' if ablation == '' else ablation[len('ablation'):]]
        ablations[name] = {}
        for paradigm in ['traditional', 'cooperative']:
            df, _ = load_latencies(os.path.join(basepath, paradigm))
            ablations[name][paradigm] = df

    paradigm_labels = {
        'traditional': 'Traditional',
        'cooperative': 'Cooperative'
    }

    txns = sorted(['P', 'D', 'N', 'O', 'S'])
    fig, axs = plt.subplots(len(ablations), 2, sharex=True, sharey=True)
    for i, (ablation, dfs) in enumerate(ablations.items()):
        for paradigm, df in dfs.items():
            x = 0 if paradigm == 'traditional' else 1
            axs[i][x].set_xlabel('Transaction latency [ms]')
            axs[i][x].set_xscale('log')
            axs[i][x].set_yscale('log')
            axs[i][x].set_xlim(10**-1.9, 1000)
            if i == 0:
                axs[i][x].set_title(paradigm_labels[paradigm])
            axs[i][x].text(x=0.99, y=0.95, s=ablation, transform=axs[i][x].transAxes, ha='right', va='top')
            plot_no = i * 2 + x + 1
            axs[i][x].text(x=0.03, y=0.7, s=f'{plot_no}', transform=axs[i][x].transAxes, ha='center', va='center', fontsize='medium' if plot_no != 10 else 'small')
            axs[i][x].add_artist(Ellipse((0.03, 0.72), 0.35 / 10.5, 0.35, fill=False, transform=axs[i][x].transAxes))
            df['begin'] = df.elapsed - df.time / 10**6
            # assign "workload phases" to each measurement
            df['phase'] = 'idle' # possible values: idle (no OLAP), olap (during OLAP query), allocation (during large allocation within OLAP query)
            for _, a in df[df.measurement == 'q09'].iterrows():
                df.loc[(df.begin <= a.elapsed) & (df.elapsed >= a.begin), 'phase'] = 'olap'
            for _, a in df[df.measurement == 'alloc'].iterrows():
                df.loc[(df.begin <= a.elapsed) & (df.elapsed >= a.begin), 'phase'] = 'alloc'
            txns_df = df[df.measurement.isin(txns)]
            # plot histogram
            hist_data = [
                txns_df[txns_df.phase == 'idle'].time / 1000,
                txns_df[txns_df.phase == 'olap'].time / 1000,
                txns_df[txns_df.phase == 'alloc'].time / 1000
            ]
            axs[i][x].hist(hist_data, bins=[0] + np.logspace(-1.9, 3, num=200, base=10).tolist(), stacked=True, color=[parameters.plot_colors['uncontended_txns'], parameters.plot_colors['concurrent_olap'], parameters.plot_colors['concurrent_alloc']], label=['Uncontended', 'Concurrent Q09', 'Concurrent Allocation'])
            # plot percentile latency
            axs[i][x].axvline(txns_df.time.quantile(0.99999) / 1000, color=parameters.plot_colors['percentile_latency'], linestyle='dashed', zorder=2.0, ymin=0, ymax=1)
            if (df.measurement == 'q09').any():
                q09_mean = df[df.measurement == 'q09'].time.mean() / 10**6
                axs[i][x].text(x=0.99, y=0.5, s=f'Q09 mean: {q09_mean:.2f} s', transform=axs[i][x].transAxes, ha='right', va='center')
            axs[i][x].set_yticks([10**1, 10**3, 10**5])
            axs[i][x].set_yticks([10**0, 10**2, 10**4, 10**6], labels=[], minor=True)
            axs[i][x].set_ylabel('')
            for lat in df[df.measurement == 'alloc'].time:
                axs[i][x].axvline(lat / 1000, color=parameters.plot_colors['alloc_latency'], zorder=2.0, ymin=0, ymax=0.2)
    fig.set_size_inches(12, 0.75 * len(ablations))
    handles, labels = axs[0][0].get_legend_handles_labels()
    fig.legend(handles, labels, ncol=3, loc='lower center')
    fig.tight_layout(rect=(0.0, 0.05, 1.0, 1.0))
    fig.subplots_adjust(wspace=0.05, hspace=0)

    fig.savefig(os.path.join(figure_path, 'latencies.png'), dpi=300)