from enum import Enum
import os

from messages import *


class JobPhase(Enum):
	not_started = 0
	mapping_ready = 1
	mapping_in_progress = 2
	reducing_ready = 3
	reducing_in_progress = 4
	done = 5


# I have no idea if this works at all
# This class is intended to communicate with a master server, or at least
# have its operations be called from the master server
class Job:
	"""
	@brief 	Job class. Specifies how a job should be distributed given a mapper, reducer
	"""
	def __init__(self, JobID, mapper, reducer, datafile, client_list):
		self.JobID = JobID
		self.Mapper = mapper
		self.Reducer = reducer
		self.phase = JobPhase.not_started
		self.client_list = client_list
		self.datafile = datafile
		self.n_clients = len(self.client_list)

	# run this before executing job operations like partition job
	def SetClientList(self, client_list):
		self.client_list = client_list
		self.n_clients = len(self.client_list)


	def GetPhase(self):
		return self.JobPhase

	def StartJob(self):
		print("JobPhase " + str(self.phase))
		self.phase = JobPhase.mapping_ready
		self.PartitionJob()

	def MappingPhase(self):
		assert (self.phase == JobPhase.mapping_ready)
		self.phase = JobPhase.mapping_in_progress
		# tell clients to begin mapping job
		# somewhere the master should be receiving this data
		# not the job of the job class to receive data and verify completion of job

	def ReducingPhase(self):
		# careful, master has to verify mapping is done
		assert (self.phase == JobPhase.mapping_in_progress)
		self.phase = JobPhase.reducing_ready
		# tell clients to begin reducing job
		# master should be receiving return data, like with mapping
		# master will verify job is done



	# TODO
	# Partition job should not send the job to the clients
	# Leave this to the server
	# Partition job SHOULD initialize a hashing algorithm to tell the server
	#	which clients should receive which data
	def PartitionJob(self):
		partitions = self.SimplePartition()
		for i, conn in enumerate(self.client_list):
			with open(partitions[i].name, 'r') as f:
				conn.send_message(DatafileMessage(self.datafile, f.read()))



	# Simple partition method that just creates separate files and distributes
	# them to the various clients
	# Partitions by key according to first letter, super simple
	# No guarantees that this will create similar sized partitions
	def SimplePartition(self):
		instream = open(self.datafile, 'r')
		fp_array = []

		for i in range(0, self.n_clients):
			fp_array.append(open('partition' + str(i), 'w'))

		for i, line in enumerate(instream):
			fp_array[i%self.n_clients].write(line)

		for f in fp_array:
			f.close()

		instream.close()
		return fp_array





