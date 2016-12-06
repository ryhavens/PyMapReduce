from PMRJob.job import hashcode
from PMRProcessing.PMRJob import PMRJob
from PMRProcessing.heartbeat.heartbeat import *


class Reducer(BeatingProcess, PMRJob):
    """
    @brief Class for reducer.
    """
    def __init__(self, reducer_cls, num_workers, heartbeat_id="Reducer", in_stream=sys.stdin, out_stream=sys.stdout):
        BeatingProcess.__init__(self)
        self.in_stream = in_stream
        self.out_stream = out_stream
        self.heartbeat_id = heartbeat_id
        self.reducer_cls = reducer_cls
        self.num_workers = num_workers

    def set_in_stream(self, in_stream):
        self.in_stream = in_stream

    def set_out_stream(self, out_stream):
        self.out_stream = out_stream

    def reduce(self):
        # TODO: Re-add heartbeat
        key_vals_map = {}
        for line in self.in_stream:
            line = line.strip()
            key, value = line.split('\t')

            if key in key_vals_map:
                key_vals_map[key].append(value)
            else:
                key_vals_map[key] = [value]

        output = []
        for key, values in sorted(key_vals_map.items(), key=lambda pair: (hashcode(pair[0]) % self.num_workers, pair[0])):
            reducer = self.reducer_cls()
            reducer.reduce(key, values, output)

        for key, value in output:
            self.out_stream.write('%s\t%s\n' % (key, value))

    def run(self):
        self.progress = 0
        self.start_time = time.time()
        self.BeginHeartbeat()
        self.reduce()
        self.EndHeartbeat()
