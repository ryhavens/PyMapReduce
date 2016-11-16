import sys
from PMRProcessing.heartbeat.heartbeat import *

class Reducer(BeatingProcess):
	"""
	@brief      Class for reducer.
	"""
	def __init__(self, heartbeat_id="Reducer", instream=sys.stdin, outstream=sys.stdout):
		BeatingProcess.__init__(self)
		self.instream = instream
		self.outstream = outstream
		self.heartbeat_id = heartbeat_id

	def SetInstream(self, instream):
		self.instream = instream

	def SetOutstream(self, outstream):
		self.outstream = outstream

	"""
	Simple count reduce
	Assumes words sorted by key
	"""
	def Reduce(self):
		self.progress = 0
		current_word = None
		current_count = 0
		word = None

		self.start_time = time.time()
		self.BeginHeartbeat()

		for line in self.instream:
			line = line.strip()
			word, count = line.split('\t')

			try:
				count = int(count)
			except ValueError:
				continue

			if (current_word == word):
				current_count += count
			else: # reached next word, output count for word
				if (current_word):
					self.outstream.write('%s\t%s\n' % (current_word, current_count))
				current_word = word
				current_count = count
			self.progress += 1

		if (word and word == current_word):
			self.outstream.write('%s\t%s\n' % (current_word, current_count))

		self.EndHeartbeat()
