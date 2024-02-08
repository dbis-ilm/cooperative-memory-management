import os
import numpy as np
import pandas as pd
from ExperimentConfiguration import ExperimentConfiguration

def load_stats(path: str):
    stats_path = os.path.join(path, 'stats.csv')
    config_path = os.path.join(path, 'config.json')
    df = pd.read_csv(stats_path)
    overlay_cols = [col for col in ['no_success_count', 'accessed_pages', 'faulted_pages', 'evicted_pages', 'dirty_write_pages', 'statm_size', 'statm_resident', 'statm_shared'] if col in df.columns]
    for col in df.columns:
        if col == 'elapsed' or col in overlay_cols:
            continue
        df[col] *= 4 * 1024  # convert pages to B
        df[col] /= 1000 ** 2 # convert B to MB
    try:
        with open(config_path, 'r') as config_file:
            config_json = config_file.read()
            config = ExperimentConfiguration.deserialize(config_json)
            # do not include warmup data
            df['elapsed'] -= config.warmup
            df = df[df['elapsed'] >= 0]
            df = df[df['elapsed'] <= config.benchmark]
            return df, config, overlay_cols
    except:
        return None

def load_latencies(path: str):
    lat_path = os.path.join(path, 'llog.csv')
    config_path = os.path.join(path, 'config.json')
    df = pd.read_csv(lat_path)
    try:
        with open(config_path, 'r') as config_file:
            config_json = config_file.read()
            config = ExperimentConfiguration.deserialize(config_json)
            # do not include warmup data
            df['elapsed'] -= config.warmup
            df = df[df['elapsed'] >= 0]
            df = df[df['elapsed'] <= config.benchmark]
            return df, config
    except:
        return None