from PMRProcessing.PMRJob import PMRJob
from PMRProcessing.heartbeat.heartbeat import *


class Reducer(BeatingProcess, PMRJob):
    """
    @brief Class for reducer.
    """
    def __init__(self, heartbeat_id="Reducer", in_stream=sys.stdin, out_stream=sys.stdout):
        BeatingProcess.__init__(self)
        self.in_stream = in_stream
        self.out_stream = out_stream
        self.heartbeat_id = heartbeat_id

    def set_in_stream(self, in_stream):
        self.in_stream = in_stream

    def set_out_stream(self, out_stream):
        self.out_stream = out_stream

    def reduce(self):
        """
        Simple count reduce
        Assumes words sorted by key
        """
        self.progress = 0
        current_word = None
        current_count = 0
        word = None

        self.start_time = time.time()
        self.BeginHeartbeat()

        for line in self.in_stream:
            line = line.strip()
            word, count = line.split('\t')

            try:
                count = int(count)
            except ValueError:
                continue

            if current_word == word:
                current_count += count
            else:  # reached next word, output count for word
                if current_word:
                    self.out_stream.write('%s\t%s\n' % (current_word, current_count))
                current_word = word
                current_count = count
            self.progress += 1

        if word and word == current_word:
            self.out_stream.write('%s\t%s\n' % (current_word, current_count))

        self.EndHeartbeat()

    def run(self):
        self.reduce()
