import sys

class Mapper:
	"""
	@brief      Class for mapper.
	"""
	def __init__(self, instream=sys.stdin, outstream=sys.stdout):
		self.instream = instream
		self.outstream = outstream

	def SetInstream(self, instream):
		self.instream = instream

	def SetOutstream(self, outstream):
		self.outstream = outstream

	"""
	Simple count map
	"""
	def Map(self):
		for line in self.instream:
			line = line.strip()
			words = line.split()
			for word in words:
				self.outstream.write('%s\t%s\n' % (word, 1))
