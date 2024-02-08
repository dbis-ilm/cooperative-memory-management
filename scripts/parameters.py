# experiment configuration
nums_threads = [1, 2, 4, 8, 16, 32, 38, 64, 76]
physical_cores = 0 # auto-configure (use all available threads on socket 0)
oltp_threads = 38
comparison_memory_limit = 2000000000

disks = {
    'NVMe': '/data2/tpcch-100.db',
    'SATA': '/data/tpcch-100.db'
}

# plot configuration
plot_colors = {
    'temporary': '#1F77B4', # tab:blue
    'bufferpool': '#EFC175',
    'bufferpool_dirty': '#CD8818', # darker shade of 'bufferpool'
    'temporary_in_use': '#9DC86A',
    'no_success_count': '#D62728', # aka OLTP throughput, tab:red
    'memory_limit': '#000000',
    'hit rate': 'tab:purple',
    'accessed_pages_x': 'deeppink',
    'accessed_pages_s': 'lime',
    'faulted_pages': 'tab:red',
    'evicted_pages': 'tab:green',
    'dirty_write_pages': 'tab:brown',
    'statm_size': 'black',
    'statm_resident': 'tab:cyan',
    'statm_shared': 'tab:olive',
    'uncontended_txns': '#1F77B4',
    'concurrent_olap': '#9DC86A',
    'concurrent_alloc': 'tab:orange',
    'percentile_latency': 'tab:red',
    'alloc_latency': 'black',
}