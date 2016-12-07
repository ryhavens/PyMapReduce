from threading import Timer
import time
import sys


class Heartbeat(Timer):
	def run(self):
		while not self.finished.is_set():
			self.function(*self.args, **self.kwargs)
			self.finished.wait(self.interval)


class BeatingProcess:
	def __init__(self, heartbeat_interval=0.001, heartbeat_stream=sys.stderr):
		self.heartbeat = Heartbeat(heartbeat_interval, self.Beat)
		self.heartbeat_stream = heartbeat_stream
		self.heartbeat_interval = heartbeat_interval
		self.progress = 0
		self.start_time = 0
		self.heartbeat_id = "BeatingProcess" # overridden by child classes
		self.BeatMethod = self.DefaultBeatMethod
		self.DieMethod = self.DefaultDieMethod

	def SetHeartbeatInterval(self, heartbeat_interval):
		self.heartbeat_interval = heartbeat_interval

	def SetHeartbeatStream(self, heartbeat_stream):
		self.heartbeat_stream = heartbeat_stream

	def SetHeartbeatID(self, heartbeat_id):
		self.heartbeat_id = heartbeat_id

	def BeginHeartbeat(self):
		self.heartbeat = Heartbeat(self.heartbeat_interval, self.Beat)
		self.heartbeat.start()

	def EndHeartbeat(self, immediate=False):
		self.heartbeat.cancel()
		if (not immediate):
			self.Beat() # beat one last time before dying
		self.DieMethod() # call die method

	# sets a function to call for writing beat to heartbeat stream
	def SetBeatMethod(self, method):
		self.BeatMethod = method

	def SetDieMethod(self, method):
		self.DieMethod = method

	def Beat(self):
		self.BeatMethod()

	def DefaultBeatMethod(self):
		self.heartbeat_stream.write('%s: Processed %d lines at %f lines per second\n' % 
			(self.heartbeat_id, self.progress, self.progress/(time.time() - self.start_time)))

	def DefaultDieMethod(self):
		self.heartbeat_stream.write('%s: Alas, I die!' % self.heartbeat_id)
