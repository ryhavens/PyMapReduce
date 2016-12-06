from PMRJob.job import hashcode
from PMRProcessing.PMRJob import PMRJob
from PMRProcessing.heartbeat.heartbeat import *
import time


class Mapper(BeatingProcess, PMRJob):
    """
    @brief Class for mapper.
    """
    def __init__(self, key, mapper_cls, num_workers, heartbeat_id="Mapper", in_stream=sys.stdin, out_stream=sys.stdout, slow_mode=False):
        BeatingProcess.__init__(self)
        self.in_stream = in_stream
        self.out_stream = out_stream
        self.heartbeat_id = heartbeat_id
        self.slow_mode = slow_mode
        self.mapper = mapper_cls()
        self.key = key
        self.num_workers = num_workers

    def set_in_stream(self, in_stream):
        self.in_stream = in_stream

    def set_out_stream(self, out_stream):
        self.out_stream = out_stream

    def map(self):
        """
        Simple count map
        """
        output = []
        for line in self.in_stream:
            if (self.slow_mode):
                time.sleep(0.1)
            self.mapper.map(self.key, line, output)
            self.progress += len(line)

        # Before spilling output to disk, mapper needs to quicksort based
        # on which partition the (key, value) pairs will be sent to
        # sort by (partitionIndex, key)
        output.sort(
            key=lambda pair: (hashcode(pair[0]) % self.num_workers, pair[0])
        )

        # Spill to disk
        for key, value in output:
            self.out_stream.write('%s\t%s\n' % (key, value))

    def run(self):
        self.progress = 0
        self.start_time = time.time()
        self.BeginHeartbeat()
        self.map()
        self.EndHeartbeat()
