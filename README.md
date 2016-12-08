# PyMapReduce
Distributed network for mapreduce built in python

Using a protocol implemented on top of TCP, we will create a structure for a flexible distributed network including a master and a series of workers that can operate efficiently in a number of different scenarios.

# Environment
The program is intended to be run on UNIX systems with Python 3.5.2

# Usage:
## Running the server:

python3 run_server.py (recommended to use -n flag)

Options:
  -h, --help            show this help message and exit
  -p PORT, --port=PORT  port to bind to
  -s HOST, --host=HOST  host address to bind to
  -n, --no-info         don't show the informational pane (useful for
                        printing)
  --slow                slow down event loop for testing

## Running a worker:
python3 run_client.py

Options:
  -h, --help            show this help message and exit
  -p PORT, --port=PORT  address of server port
  -s HOST, --host=HOST  server host address

## Submitting a job:
python3 submit_job.py

Options:
  -h, --help            show this help message and exit
  -p PORT, --port=PORT  port to bind to
  -s HOST, --host=HOST  host address to bind to
  -m MAPPER, --mapper=MAPPER
                        the mapper package path
  -r REDUCER, --reducer=REDUCER
                        the reducer package path
  -d DATAFILE, --datafile=DATAFILE
                        the datafile path

# Running a test
Example:
In one terminal: python3 run_server.py -p 5000
In another terminal: python3 run_client.py -p 5000
In yet another terminal: python3 submit_job.py -d brown.txt -p 5000
After that completes: python3 check_output.py -d right.txt

# Creating a Custom Job
For creating your own custom jobs, use the PMRProcessing/mapper/word_count_mapper.py and the PMRProcessing/reducer/word_count_reducer.py as templates. All that is needed is to reimplement the map and reduce functions.
