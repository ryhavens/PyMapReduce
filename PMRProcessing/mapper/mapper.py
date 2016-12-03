import sys
from PMRProcessing.heartbeat.heartbeat import *

class Mapper(BeatingProcess):
	"""
	@brief      Class for mapper.
	"""
	def __init__(self, heartbeat_id="Mapper", instream=sys.stdin, outstream=sys.stdout):
		BeatingProcess.__init__(self)
		self.instream = instream
		self.outstream = outstream
		self.heartbeat_id = heartbeat_id

	def SetInstream(self, instream):
		self.instream = instream

	def SetOutstream(self, outstream):
		self.outstream = outstream

	"""
	Simple count map
	"""
	def Map(self):
		self.progress = 0
		self.start_time = time.time()
		self.BeginHeartbeat()
		for line in self.instream:
			line = line.strip()
			words = line.split()
			for word in words:
				self.outstream.write('%s\t%s\n' % (word, 1))
				self.progress += 1
		self.EndHeartbeat()

	def run(self):
		self.Map()



