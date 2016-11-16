from PMRProcessing.mapper.mapper import Mapper
from PMRProcessing.reducer.reducer import Reducer
import os

brown = open('brown.txt','r')
f1 = open('f1.txt', 'w')
mapper = Mapper(instream=brown, outstream=f1)
mapper.Map()
f1.close()

os.system('cat f1.txt | sort -k1,1 > f2.txt')

f2 = open('f2.txt', 'r')
reducer = Reducer(instream=f2, outstream=open('/dev/null','w'))
reducer.Reduce()
f2.close()


class Job:
	def __init__(self, JobID, MyMapper=None, MyReducer=None):
		self.JobID = JobID
		self.Mapper = MyMapper
		self.Reducer = MyReducer

	def SetMapper(self, MyMapper):
		self.Mapper = MyMapper

	def SetReducer(self, MyReducer):
		self.Reducer = MyReducer

	def ExecMapper(self):
		self.Mapper.Map()

	def ExecReducer(self):
		self.Reducer.Reduce()