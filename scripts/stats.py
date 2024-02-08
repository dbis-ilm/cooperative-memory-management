# %% ipynb
from analysis import load_stats
from glob import glob
import parameters
import matplotlib.pyplot as plt
import numpy as np
import os

SEARCH_DIR = '../results/'
dfs = {}
memory_limits = {}
overlays = {}
for file in [y for x in os.walk(SEARCH_DIR) for y in glob(os.path.join(x[0], 'stats.csv'))]:
    name = os.path.dirname(file[len(SEARCH_DIR):]).replace('/', '-')
    df, config, overlay_cols = load_stats(os.path.dirname(file))
    dfs[name] = df
    memory_limits[name] = config.memory_limit / 1000**2
    overlays[name] = overlay_cols

MAX_HORIZONTAL_PLOTS = 4
num_plots = 3
num_rows = (len(dfs) + MAX_HORIZONTAL_PLOTS - 1) // MAX_HORIZONTAL_PLOTS
num_cols = min(len(dfs), MAX_HORIZONTAL_PLOTS)
fig, axs = plt.subplots(num_plots * num_rows, num_cols, squeeze=False)
overlay_handles={}
for c, (name, df) in enumerate(dfs.items()):
    try:
        overlay_columns = overlays[name]
        stackplot_cols = ['temp_pages', 'data_pages']
        stackplot_labels = ['Reserved temporary memory', 'Buffer pool memory']
        stackplot_colors = [parameters.plot_colors['temporary'], parameters.plot_colors['bufferpool']]
        if 'dirty_data_pages' in df.columns:
            df.data_pages -= df.dirty_data_pages
            stackplot_cols.append('dirty_data_pages')
            stackplot_labels.append('Buffer pool memory (dirty)')
            stackplot_colors.append(parameters.plot_colors['bufferpool_dirty'])
        for i in range(num_plots):
            ax = axs[i + num_plots * (c // MAX_HORIZONTAL_PLOTS)][c % MAX_HORIZONTAL_PLOTS]
            ax.stackplot(df['elapsed'], df[stackplot_cols].T, labels=stackplot_labels, colors=stackplot_colors)
            ax.stackplot(df['elapsed'], df[['temp_in_use']].T, labels=['Used temporary memory'], colors=[parameters.plot_colors['temporary_in_use']])
            if i == 0: # throughput plot
                # plot transaction throughput
                overlay_col = 'no_success_count'
                overlay_y = []
                THROUGHPUT_WINDOW = 0.5 if df['elapsed'].max() < 200 else 5 # compute average throughput for 'THROUGHPUT_WINDOW' second windows
                overlay_x = np.arange(df['elapsed'].min(), df['elapsed'].max() - THROUGHPUT_WINDOW, THROUGHPUT_WINDOW)
                for start in overlay_x:
                    overlay_y.append(df[df['elapsed'].between(start, start + THROUGHPUT_WINDOW)][overlay_col].sum() * (60 / THROUGHPUT_WINDOW) / 10**6)
                ax2 = ax.twinx()
                ax2.set_ylabel('OLTP throughput [MtpmC]')  # we already handled the x-label with ax1
                throughput_handle = ax2.plot(overlay_x, overlay_y, color=parameters.plot_colors[overlay_col])
                ax2.tick_params(axis='y')
                ax2.set_ylim((0, 2))
            elif i == 1: # buffer pool stats plot
                # plot page faults/s, evictions/s, dirty writes/s
                OVERLAY_WINDOW = 0.5 if df['elapsed'].max() < 200 else 5 # compute average page faults/s for 'OVERLAY_WINDOW' second windows
                overlay_x = np.arange(df['elapsed'].min(), df['elapsed'].max() - OVERLAY_WINDOW, OVERLAY_WINDOW)
                ax2 = ax.twinx()
                for overlay_col in set(['faulted_pages', 'dirty_write_pages', 'evicted_pages']) & set(overlay_columns):
                    overlay_y = []
                    for start in overlay_x:
                        overlay_y.append(df[df['elapsed'].between(start, start + OVERLAY_WINDOW)][overlay_col].sum() / OVERLAY_WINDOW)
                    overlay_handles[overlay_col] = ax2.plot(overlay_x, overlay_y, color=parameters.plot_colors[overlay_col])
                ax2.set_ylabel('[1/s]')  # we already handled the x-label with ax1
                ax2.tick_params(axis='y')
            elif i == 2: # OS memory stats & cache hit rate plot
                # plot statm total size, resident size, shared size
                OVERLAY_WINDOW = 0.5 if df['elapsed'].max() < 200 else 5 # compute averages for 'OVERLAY_WINDOW' second windows
                overlay_x = np.arange(df['elapsed'].min(), df['elapsed'].max() - OVERLAY_WINDOW, OVERLAY_WINDOW)
                for overlay_col in set(['statm_size', 'statm_resident', 'statm_shared']) & set(overlay_columns):
                    overlay_y = []
                    for start in overlay_x:
                        overlay_y.append(df[df['elapsed'].between(start, start + OVERLAY_WINDOW)][overlay_col].mean() / 1000**2)
                    overlay_handles[overlay_col] = ax.plot(overlay_x, overlay_y, color=parameters.plot_colors[overlay_col])
                # plot cache hit rate on second y-axis
                if 'accessed_pages' in overlay_columns and 'faulted_pages' in overlay_columns:
                    ax2 = ax.twinx()
                    overlay_y = []
                    for start in overlay_x:
                        sub_df = df[df['elapsed'].between(start, start + OVERLAY_WINDOW)]
                        overlay_y.append(100.0 - sub_df['faulted_pages'].sum() / sub_df['accessed_pages'].sum() * 100)
                    overlay_handles['hit rate'] = ax2.plot(overlay_x, overlay_y, color=parameters.plot_colors['hit rate'])
                    ax2.set_ylabel('hit rate [%]')  # we already handled the x-label with ax1
                    ax2.tick_params(axis='y')
                    ax2.set_ylim(min(overlay_y), 100)
            ax.set_xlabel('Elapsed time [s]')
            ax.set_ylabel('Data Volume [MB]')
            if memory_limits[name] != None:
                ax.axhline(y=memory_limits[name], color=parameters.plot_colors['memory_limit'], linestyle='dashed')
            if i == 0:
                ax.set_title(name)
            ax.set_ylim(0, max(df[['temp_pages', 'data_pages']].sum(axis=1).max(), df['statm_resident'].max() / 1000**2 * 1.05 if 'statm_resident' in df.columns else 0, memory_limits[name] * 1.05))
    except Exception as e:
        print(f'Could not plot {name}: {e}')

handles, labels = axs[0][0].get_legend_handles_labels()
handles.append(throughput_handle[0])
labels.append('OLTP Throughput')
for col, handle in overlay_handles.items():
    handles.append(handle[0])
    labels.append(col)
fig.legend(handles, labels, ncol=4, loc='lower center')
fig.set_size_inches(w=6 * num_cols, h=3 * num_plots * num_rows)
fig.tight_layout(rect=(0.0, 0.1 / num_rows, 1.0, 1.0))