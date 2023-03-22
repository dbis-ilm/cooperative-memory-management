#!/usr/bin/env python3

import matplotlib.pyplot as plt
import os
import pandas as pd
import struct

search_dir = 'build'

for fname in os.listdir(search_dir):
    name, ext = os.path.splitext(fname)
    if ext != '.trc':
        continue

    path = os.path.join(search_dir, fname)
    print(f'Processing trace {path}')
    data = []
    with open(path, 'rb') as file:
        file.seek(0, 2)
        len = file.tell()
        file.seek(0, 0)
        read = 0
        while read != len:
            timestamp, pid_action = struct.unpack('dQ', file.read(16))
            read += 16
            data.append((timestamp, pid_action & 0x00ffffffffffffff, pid_action >> 56))

    df = pd.DataFrame(data, columns=['elapsed time [s]', 'pid', 'action'])

    fig, ax = plt.subplots(1, 1)
    df = df[df.pid < 2*10**6] # filter out temp data for now
    # see https://stackoverflow.com/questions/39753282/scatter-plot-with-single-pixel-marker-in-matplotlib for plotting each trace entry as a single pixel
    fig.set_size_inches(12, 6)
    fig.set_dpi(400.0)
    df[df.action == 1].plot.scatter('elapsed time [s]', 'pid', c='red', lw=0, marker='o', s=(72./fig.dpi)**2, ax=ax)    # evictions
    df[df.action == 2].plot.scatter('elapsed time [s]', 'pid', c='green', lw=0, marker='o', s=(72./fig.dpi)**2, ax=ax)  # faults
    fig.tight_layout()
    save_path = f'{name}.png'
    print(f'Saving plot to {save_path}')
    fig.savefig(save_path, dpi=400)