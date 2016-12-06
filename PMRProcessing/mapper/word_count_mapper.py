from PMRProcessing.PMRJob import PMRJob
from PMRProcessing.heartbeat.heartbeat import *
import time


class Mapper(BeatingProcess, PMRJob):
    """
    @brief Class for mapper.
    """
    def __init__(self, heartbeat_id="Mapper", 
                       in_stream=sys.stdin, 
                       out_stream=sys.stdout,
                       slow_mode=False):
        BeatingProcess.__init__(self)
        self.in_stream = in_stream
        self.out_stream = out_stream
        self.heartbeat_id = heartbeat_id
        self.slow_mode = slow_mode

    def set_in_stream(self, in_stream):
        self.in_stream = in_stream

    def set_out_stream(self, out_stream):
        self.out_stream = out_stream

    def map(self):
        """
        Simple count map
        """
        for line in self.in_stream:
            if (self.slow_mode):
                time.sleep(0.1)
            line = line.strip()
            words = line.split()
            for word in words:
                self.out_stream.write('%s\t%s\n' % (word, 1))
            self.progress += len(line)

    def run(self):
        self.progress = 0
        self.start_time = time.time()
        self.BeginHeartbeat()
        self.map()
        self.EndHeartbeat()
