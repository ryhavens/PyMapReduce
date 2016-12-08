import os

from filesystems import SimpleFileSystem
from math import ceil, inf


def chunk_input_data(data_path, lines_per_partition=500):
    """
    Create n partitions of 500 lines each
    :return:
    """
    partition = -1
    partition_paths = []
    partition_handlers = []

    fs = SimpleFileSystem()
    with fs.open(data_path, 'r') as f:
        for index, line in enumerate(f):
            if index % lines_per_partition == 0:
                # Clean up open descriptor
                if partition_handlers:
                    fs.close(partition_handlers[partition])

                # Create new partition
                partition += 1
                partition_paths.append(fs.get_writeable_file_path())
                partition_handlers.append(fs.open(partition_paths[partition], 'w'))

            partition_handlers[partition].write(line)

    fs.close(partition_handlers[partition])

    print (partition_paths)
    return partition_paths


def chunk_input_data_by_size_and_workers(data_path, n_workers):
    """
    Create n_workers partitions of roughly equal number of bytes
    :return:
    """
    partition = -1
    partition_paths = []
    partition_handlers = []


    # TODO main server should be aware of these values as they impact the 
    # progress measure
    file_size = os.path.getsize(data_path)
    # round up since rounding down can cause more chunks than workers (very bad)
    chunk_size = ceil(file_size / (1.0*n_workers))

    fs = SimpleFileSystem()
    bytes_chunked = inf # immediately create partition
    with fs.open(data_path, 'r') as f:
        for line in f:
            if bytes_chunked >= chunk_size:
                # Clean up open descriptor
                if partition_handlers:
                    fs.close(partition_handlers[partition])

                # Create new partition
                partition += 1
                partition_paths.append(fs.get_writeable_file_path())
                partition_handlers.append(fs.open(partition_paths[partition], 'w'))

                bytes_chunked = 0

            partition_handlers[partition].write(line)
            bytes_chunked += len(line) # assuming char is 1 byte (hopefully a safe assumption)

    fs.close(partition_handlers[partition])

    return partition_paths 


def set_result_file(job, path):
    job.result_file = path


class SubJob:
    """
    Represents a piece of the job that will be sent to a worker

    When using a SubJob, pre_execute and post_execute should be run
    before and after the job respectively
    """
    def __init__(self,
                 id,
                 instruction_path,
                 instruction_type,
                 data_path=None,
                 num_workers=0,

                 # Used by reducers to denote their output files
                 partition_num=0,

                 # Anything to do before running, gets passed this instance
                 # Can be a single function or a list
                 # If data_path is not set on create, do_before must set it
                 do_before=None,

                 # Passed self, output_path
                 # Can be a single function or a list of functions
                 do_after=None
                 ):
        self.id = id
        self.instruction_path = instruction_path
        self.instruction_type = instruction_type
        self.data_path = data_path
        self.num_workers = num_workers
        self.partition_num = partition_num

        self.do_before = do_before
        self.do_after = do_after

        self.result_file = None

        self.client = None
        self.pending_assignment = True

        self.done = False

    def __str__(self):
        return '<SubJob: id={id} instruction_type={instruction_type} data_path={data_path} client={client} partition_num={partition_num} pending_assignment={assigned} done={done}'.format(
            id=self.id,
            instruction_type=self.instruction_type,
            data_path=self.data_path,
            client=self.client,
            partition_num=self.partition_num,
            assigned = self.pending_assignment,
            done = self.done
        )

    def pre_execute(self):
        """
        Run any do_before methods that were specified
        :return:
        """
        if self.do_before:
            if type(self.do_before) is list:
                for action in self.do_before:
                    action(self)
            else:
                self.do_before(self)

    def post_execute(self, output_path):
        """
        Run any do_after methods that were specified
        Then pass the output_path to any jobs that
        depend on it
        :param output_path:
        :return:
        """
        self.done = True
        if type(self.do_after) is list:
            for action in self.do_after:
                action(self, output_path)
