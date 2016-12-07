from PMRJob.job import hashcode
from PMRProcessing.PMRJob import PMRJob
from PMRProcessing.heartbeat.heartbeat import *
import time

from filesystems import SimpleFileSystem


class Mapper(BeatingProcess, PMRJob):
    """
    @brief Class for mapper.
    """
    def __init__(self, key, mapper_cls, num_workers, heartbeat_id="Mapper", in_stream=sys.stdout, slow_mode=False):
        BeatingProcess.__init__(self)
        self.heartbeat_id = heartbeat_id
        self.in_stream = in_stream
        self.slow_mode = slow_mode
        self.mapper = mapper_cls()
        self.key = key
        self.num_workers = num_workers

    def map(self):
        """
        Simple count map
        """
        output = []
        for line in self.in_stream:
            # can set this higher or lower for slower speeds
            # this value makes speeds ~10 times slower
            if (self.slow_mode):
                for i in range(1000):
                    continue
            self.mapper.map(self.key, line, output)
            self.progress += len(line)

        # Before spilling output to disk, mapper needs to quicksort based
        # on which partition the (key, value) pairs will be sent to
        # sort by (partitionIndex, key)
        output.sort(
            key=lambda pair: (hashcode(pair[0]) % self.num_workers, pair[0])
        )

        # Spill to disk
        partition_files = []
        sf = SimpleFileSystem()
        for i in range(self.num_workers):
            partition_files.append(
                sf.open(sf.get_mapper_output_file(i), 'w')
            )

        for key, value in output:
            if (self.slow_mode):
                for i in range(1000):
                    continue
            self.progress += len(line)
            partition_num = hashcode(key) % self.num_workers
            partition_files[partition_num].write('%s\t%s\n' % (key, value))

        for partition_file in partition_files:
            sf.close(partition_file)

    def run(self):
        self.progress = 0
        self.start_time = time.time()
        self.BeginHeartbeat()
        self.map()
        self.EndHeartbeat()
