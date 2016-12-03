from PMRProcessing.PMRJob import PMRJob
from PMRProcessing.heartbeat.heartbeat import *


class Mapper(BeatingProcess, PMRJob):
    """
    @brief Class for mapper.
    """
    def __init__(self, heartbeat_id="Mapper", in_stream=sys.stdin, out_stream=sys.stdout):
        BeatingProcess.__init__(self)
        self.in_stream = in_stream
        self.out_stream = out_stream
        self.heartbeat_id = heartbeat_id

    def set_in_stream(self, in_stream):
        self.in_stream = in_stream

    def set_out_stream(self, out_stream):
        self.out_stream = out_stream

    def map(self):
        """
        Simple count map
        """
        self.progress = 0
        self.start_time = time.time()
        self.BeginHeartbeat()
        for line in self.in_stream:
            line = line.strip()
            words = line.split()
            for word in words:
                self.out_stream.write('%s\t%s\n' % (word, 1))
                self.progress += 1
        self.EndHeartbeat()

    def run(self):
        self.map()
