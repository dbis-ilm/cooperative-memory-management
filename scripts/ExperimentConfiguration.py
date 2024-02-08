import json
import os

class ExperimentConfiguration(object):
    def __init__(self, **kwargs):
        self.numactl_args = kwargs.get('numactl_args', '-c 0')
        self.binary = kwargs.get('binary', 'frontend/tpcch')
        self.database_path = kwargs.get('database_path', '/data2/tpcch-100.db') # TODO maybe there should be no default value for this?
        self.dataset_path = kwargs.get('dataset_path', 'data/tpcch/100') # TODO default?
        self.warmup = kwargs.get('warmup', 15)
        self.benchmark = kwargs.get('benchmark', 120)
        self.oltp = kwargs.get('oltp', 38)
        self.olap = kwargs.get('olap', 'none')
        self.olap_interval = kwargs.get('olap_interval', 30)
        self.olap_stdout = kwargs.get('olap_stdout', False)
        self.parallel = kwargs.get('parallel', 0)
        self.sandbox = kwargs.get('sandbox', True)
        self.no_dirty_writeback = kwargs.get('no_dirty_writeback', False)
        self.no_async_flush = kwargs.get('no_async_flush', False)
        self.no_eviction_target = kwargs.get('no_eviction_target', False)
        self.memory_limit = kwargs.get('memory_limit', 17179869184) # 16 GiB
        self.partitioning_strategy = kwargs.get('partitioning_strategy', 'basic')
        self.partitioned_num_temp_pages = kwargs.get('partitioned_num_temp_pages', None)

    def __str__(self) -> str:
        return str(self.__dict__)

    def serialize(self) -> str:
        return json.dumps(self.__dict__, sort_keys=True, indent=4)

    def deserialize(str: str):
        return ExperimentConfiguration(**json.loads(str))

    def to_commandline(self, output_path: str) -> str:
        full_binary_path = os.path.join('build', self.binary)
        cmd = f'numactl {self.numactl_args} {full_binary_path} {self.database_path} --ch_path={self.dataset_path} --memory_limit={self.memory_limit} --warmup={self.warmup} --benchmark={self.benchmark} --oltp={self.oltp} --olap={self.olap} --olap_interval={self.olap_interval} --parallel={self.parallel} --partitioning_strategy={self.partitioning_strategy}'
        if self.partitioned_num_temp_pages != None:
            cmd += f' --partitioned_num_temp_pages={self.partitioned_num_temp_pages}'
        if self.olap_stdout:
            cmd += ' --olap_stdout'
        if self.sandbox:
            cmd += ' --sandbox'
        if self.no_dirty_writeback:
            cmd += ' --no_dirty_writeback'
        if self.no_async_flush:
            cmd += ' --no_async_flush'
        if self.no_eviction_target:
            cmd += ' --no_eviction_target'
        stats_path = os.path.join(output_path, 'stats.csv')
        llog_path = os.path.join(output_path, 'llog.csv')
        cmd += f' --collect_stats={stats_path}'
        cmd += f' --latency_log={llog_path}'
        return cmd