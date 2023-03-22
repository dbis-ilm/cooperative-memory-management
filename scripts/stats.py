# %% ipynb
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

SEARCH_DIR = '../results/real_olap'
dfs = {}
for file in os.listdir(SEARCH_DIR):
    if file.startswith('stats') and file.endswith('.csv'):
        name = ''
        if '_' in file:
            name = file.split('_', maxsplit=1)[-1].rsplit('.', maxsplit=1)[0]
        df = pd.read_csv(os.path.join(SEARCH_DIR, file))
        overlay_columns = [col for col in ['no_success_count', 'faulted_pages'] if col in df.columns]
        for col in df.columns:
            if col == 'elapsed' or col in overlay_columns:
                continue
            df[col] *= 4 * 1024  # convert pages to B
            df[col] /= 1000 ** 2 # convert B to MB

        # do not plot warmup data
        df['elapsed'] -= 60
        df = df[df['elapsed'] >= 0]
        dfs[name] = df

num_plots = len(overlay_columns)
fig, axs = plt.subplots(num_plots, len(dfs), squeeze=False)
for c, (name, df) in enumerate(dfs.items()):
    for i, overlay_col in enumerate(overlay_columns):
        ax = axs[i][c]
        ax.stackplot(df['elapsed'], df[['temp_pages', 'data_pages']].T, labels=['Reserved temporary memory', 'Buffer pool memory'])
        for col in df.columns:
            if col in ['elapsed', 'data_pages', 'temp_pages'] + overlay_columns:
                continue
            #y = df[col] + df['data_pages'] if col == 'temp_in_use' else df[col]
            y = df[[col]].T
            ax.stackplot(df['elapsed'], df[['temp_in_use']].T, labels=['Used temporary memory'])
        overlay_color = 'tab:red'
        overlay_y = []
        overlay_ylim = (None, None)
        if overlay_col == 'no_success_count':
            # plot transaction throughput
            THROUGHPUT_WINDOW = 0.5 # compute average throughput for 'THROUGHPUT_WINDOW' second windows
            overlay_x = np.arange(df['elapsed'].min(), df['elapsed'].max() - THROUGHPUT_WINDOW, THROUGHPUT_WINDOW)
            for start in overlay_x:
                overlay_y.append(df[df['elapsed'].between(start, start + THROUGHPUT_WINDOW)][overlay_col].sum() * (60 / THROUGHPUT_WINDOW) / 10**6)
            overlay_label = 'OLTP throughput [MtpmC]'
            overlay_ylim = (0, 3)
        elif overlay_col == 'faulted_pages':
            # plot page faults/s
            FAULT_WINDOW = 0.1 # compute average page faults/s for 'FAULT_WINDOW' second windows
            overlay_x = np.arange(df['elapsed'].min(), df['elapsed'].max() - FAULT_WINDOW, FAULT_WINDOW)
            for start in overlay_x:
                overlay_y.append(df[df['elapsed'].between(start, start + FAULT_WINDOW)][overlay_col].sum() / FAULT_WINDOW)
            overlay_label = 'page faults [1/s]'
            overlay_ylim = (0, 500000)
        ax2 = ax.twinx()
        ax2.set_ylabel(overlay_label)  # we already handled the x-label with ax1
        overlay_handle = ax2.plot(overlay_x, overlay_y, color=overlay_color)
        ax2.tick_params(axis='y')
        ax2.set_ylim(overlay_ylim)
        ax.set_xlabel('Elapsed time [s]')
        ax.set_ylabel('Data Volume [MB]')
        if i == 0:
            ax.set_title(name)
        ax.set_ylim(0, 2000)

handles, labels = axs[0][0].get_legend_handles_labels()
handles.append(overlay_handle[0])
labels.append('OLTP Throughput')
fig.legend(handles, labels, ncol=4, loc='lower center')
fig.set_size_inches(w=6 * len(dfs), h=3 * num_plots)
fig.tight_layout(rect=(0.0, 0.1, 1.0, 1.0))