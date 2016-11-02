# PyMapReduce
Distributed network for mapreduce built in python

Using a protocol implemented on top of TCP, we will create a structure for a flexible distributed network including a master and a series of workers that can operate efficiently in a number of different scenarios.


Message format

* 1 byte message type

* 4 bytes data length

* data

Message types:

* 1: Subscribe data=UUID

* 2: Subscribe Ack