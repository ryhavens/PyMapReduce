import os

from filesystems import SimpleFileSystem


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
        if type(self.do_after) is list:
            for action in self.do_after:
                action(self, output_path)
