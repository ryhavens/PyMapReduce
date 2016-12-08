from PMRJob.job import hashcode
from PMRProcessing.PMRJob import PMRJob
from PMRProcessing.heartbeat.heartbeat import *
import time

from filesystems import SimpleFileSystem


class Reducer(BeatingProcess, PMRJob):
    """
    @brief Class for reducer.
    """
    def __init__(self, reducer_cls, num_workers, partition_num, heartbeat_id="Reducer", slow_mode=False):
        BeatingProcess.__init__(self)
        self.heartbeat_id = heartbeat_id
        self.slow_mode = slow_mode
        self.reducer_cls = reducer_cls
        self.num_workers = num_workers
        self.slow_mode = slow_mode
        self.partition_num = partition_num

    def reduce(self):
        key_vals_map = {}
        fs = SimpleFileSystem()
        files = fs.get_partition_files(self.partition_num)

        for file_path in files:
            file = fs.open(file_path, 'r')
            for line in file:
                line = line.strip()
                key, value = line.split('\t')

                if (self.slow_mode):
                    time.sleep(0.001)

                if key in key_vals_map:
                    key_vals_map[key].append(value)
                else:
                    key_vals_map[key] = [value]

                self.progress += len(line)

        output = []
        for key, value in sorted(key_vals_map.items(), key=lambda pair: (hashcode(pair[0]) % self.num_workers, pair[0])):
            if (self.slow_mode):
                time.sleep(0.001)
            reducer = self.reducer_cls()
            reducer.reduce(key, value, output)
            self.progress += len(key)

        output_file = fs.open(fs.get_output_file(self.partition_num), 'w')
        for key, value in output:
            output_file.write('%s\t%s\n' % (key, value))
        fs.close(output_file)

    def run(self):
        self.progress = 0
        self.start_time = time.time()
        self.BeginHeartbeat()
        self.reduce()
        self.EndHeartbeat()
