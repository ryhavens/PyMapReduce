import sys

class Reducer:
	"""
	@brief      Class for reducer.
	"""
	def __init__(self, instream=sys.stdin, outstream=sys.stdout):
		self.instream = instream
		self.outstream = outstream

	def SetInstream(self, instream):
		self.instream = instream

	def SetOutstream(self, outstream):
		self.outstream = outstream

	"""
	Simple count reduce
	Assumes words sorted by key
	"""
	def Reduce(self):
		current_word = None
		current_count = 0
		word = None

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
		if (word and word == current_word):
			self.outstream.write('%s\t%s\n' % (current_word, current_count))
