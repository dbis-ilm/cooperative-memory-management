# %% ipynb
# analyze latency log by plotting average transaction latencies in 'AVG_WINDOW' windows over time
from analysis import load_latencies
import os
import numpy as np
import matplotlib.pyplot as plt

ablation_labels = {
    '-inmemory': 'in-memory',
    '+writeback': 'naïve writes',
    '+evictiontarget': '+ eviction target',
    '+idlewriters': '+ idle writers',
    'full': '+ both',
}

# load data
ablations = {}
path = '../results/trad-coop/NVMe/none/'
for ablation in [subdir for subdir in os.listdir(path) if 'ablation' in subdir] + ['']:
    basepath = os.path.join(path, ablation)
    if not 'inmemory' in ablation:
        continue
    name = (ablation_labels['full' if ablation == '' else ablation[len('ablation'):]]) + ' (OLTP only)'
    ablations[name] = {}
    for paradigm in ['traditional', 'cooperative']:
        df, _ = load_latencies(os.path.join(basepath, paradigm))
        ablations[name][paradigm] = df

path = '../results/trad-coop/NVMe/q09/'
for ablation in [subdir for subdir in os.listdir(path) if 'ablation' in subdir] + ['']:
    basepath = os.path.join(path, ablation)
    name = ablation_labels['full' if ablation == '' else ablation[len('ablation'):]]
    ablations[name] = {}
    for paradigm in ['traditional', 'cooperative']:
        df, _ = load_latencies(os.path.join(basepath, paradigm))
        ablations[name][paradigm] = df

# %%
txn_labels = {
    'D': 'Delivery',
    'N': 'NewOrder',
    'O': 'OrderStatus',
    'P': 'Payment',
    'S': 'StockSelect',
}
paradigm_labels = {
    'traditional': 'Traditional',
    'cooperative': 'Cooperative'
}

txns = sorted(['P', 'D', 'N', 'O', 'S'])
fig, axs = plt.subplots(len(ablations), 2, sharex=True, sharey=True)
min = 10**-1.9
max = 1000
for i, (ablation, dfs) in enumerate(ablations.items()):
    for paradigm, df in dfs.items():
        x = 0 if paradigm == 'traditional' else 1
        axs[i][x].set_xlabel('Transaction latency [ms]')
        axs[i][x].set_xscale('log') ###
        axs[i][x].set_yscale('log')
        axs[i][x].set_xlim(min, max)
        if i == 0:
            axs[i][x].set_title(paradigm_labels[paradigm])
        axs[i][x].text(x=0.99, y=0.95, s=ablation, transform=axs[i][x].transAxes, ha='right', va='top')
        for t in txns:
            sub_df = df[df.measurement == t]
            (sub_df.time / 1000).plot.hist(ax=axs[i][x], bins=[0] + np.logspace(-1.9, 3, num=200, base=10).tolist(), alpha=0.3, label=t)
        if (df.measurement == 'q09').any():
            q09_med = df[df.measurement == 'q09'].time.mean() / 10**6
            axs[i][x].text(x=0.99, y=0.5, s=f'Q09 mean: {q09_med:.2f} s', transform=axs[i][x].transAxes, ha='right', va='center')
        axs[i][x].set_ylabel(None)
        for lat in df[df.measurement == 'alloc'].time:
            axs[i][x].axvline(lat / 1000, color='black', zorder=-1.0, ymin=0.95, ymax=1)
        if False: # TODO: add back support for plotting latencies over time
            # plot average transaction latencies in 'AVG_WINDOW' windows over time
            AVG_WINDOW = 0.5 if df['elapsed'].max() < 200 else 5
            x = np.arange(df['elapsed'].min(), df['elapsed'].max() - AVG_WINDOW, AVG_WINDOW)
            for t in txns:
                y = []
                sub_df = df[df.measurement == t]
                for start in x:
                    y.append(sub_df[sub_df['elapsed'].between(start, start + AVG_WINDOW)]['time'].mean() / 1000)
                axs[i][x].plot(x, y, label=t)
                axs[i][x].legend()
                axs[i][x].set_xlabel('Elapsed time [s]')
                axs[i][x].set_ylabel('Average latency [ms]')
                axs[i][x].set_yscale('log')
fig.set_size_inches(12, 0.75 * len(ablations))
handles, labels = axs[0][0].get_legend_handles_labels()
fig.legend(handles, [txn_labels[t] for t in labels], ncol=5, loc='lower center')
fig.tight_layout(rect=(0.0, 0.05, 1.0, 1.0))
fig.subplots_adjust(wspace=0.05, hspace=0)