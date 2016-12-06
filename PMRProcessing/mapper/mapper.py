from PMRProcessing.PMRJob import PMRJob
from PMRProcessing.heartbeat.heartbeat import *


class Mapper(BeatingProcess, PMRJob):
    """
    @brief Class for mapper.
    """
    def __init__(self, key, mapper_cls, heartbeat_id="Mapper", in_stream=sys.stdin, out_stream=sys.stdout):
        BeatingProcess.__init__(self)
        self.in_stream = in_stream
        self.out_stream = out_stream
        self.heartbeat_id = heartbeat_id
        self.mapper = mapper_cls()
        self.key = key

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
            self.mapper.map(self.key, line, output)
            self.progress += len(line)

        for key, value in output:
            self.out_stream.write('%s\t%s\n' % (key, value))

    def run(self):
        self.progress = 0
        self.start_time = time.time()
        self.BeginHeartbeat()
        self.map()
        self.EndHeartbeat()
