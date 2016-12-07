from PMRJob.job import hashcode
from PMRProcessing.PMRJob import PMRJob
from PMRProcessing.heartbeat.heartbeat import *
import time


class Reducer(BeatingProcess, PMRJob):
    """
    @brief Class for reducer.
    """
    def __init__(self, reducer_cls, num_workers, heartbeat_id="Reducer", in_stream=sys.stdin, out_stream=sys.stdout, slow_mode=False):
        BeatingProcess.__init__(self)
        self.in_stream = in_stream
        self.out_stream = out_stream
        self.heartbeat_id = heartbeat_id
        self.slow_mode = slow_mode
        self.reducer_cls = reducer_cls
        self.num_workers = num_workers

    def set_in_stream(self, in_stream):
        self.in_stream = in_stream

    def set_out_stream(self, out_stream):
        self.out_stream = out_stream

    def reduce(self):
        key_vals_map = {}
        for line in self.in_stream:
            line = line.strip()
            key, value = line.split('\t')
            
            if (self.slow_mode):
                for i in range(1000):
                    continue

            if key in key_vals_map:
                key_vals_map[key].append(value)
            else:
                key_vals_map[key] = [value]

            self.progress += len(line)

        output = []
        for key, value in sorted(key_vals_map.items(), key=lambda pair: (hashcode(pair[0]) % self.num_workers, pair[0])):
            reducer = self.reducer_cls()
            reducer.reduce(key, value, output)
            self.progress += len(key)

        for key, value in output:
            if (self.slow_mode):
                for i in range(1000):
                    continue
            self.progress += len(key)
            self.out_stream.write('%s\t%s\n' % (key, value))

    def run(self):
        self.progress = 0
        self.start_time = time.time()
        self.BeginHeartbeat()
        self.reduce()
        self.EndHeartbeat()
