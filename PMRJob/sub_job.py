import os

from filesystems import SimpleFileSystem


def fixed_size_partition(data_path, lines_per_partition=500):
    """
    Create n partitions of 500 lines each
    :return:
    """
    print('test')
    print('test')
    print('test')
    print('test')
    print('test')
    print('test')
    print('test')
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

    print(partition_paths)
    return partition_paths


def stitch_data_files(job):
    """
    Stitch the data_paths of a job together so that it can work on them
    Expects job.data_paths_list to be set
    Sets job.data_path to single file on finish
    :param self:
    :param job:
    :return:
    """
    fs = SimpleFileSystem()
    out_file_path = fs.get_writeable_file_path()
    with fs.open(out_file_path, 'w') as f:
        for path in job.data_paths_list:
            with fs.open(path, 'r') as g:
                f.write(g.read())
    job.data_path = out_file_path


def sort_data_file(job):
    """
    Sorts the job.data_path file
    :param self:
    :param job:
    :return:
    """
    fs = SimpleFileSystem()
    in_path = fs.get_writeable_file_path()
    os.system('mv {} {}'.format(job.data_path, in_path))
    os.system('cat {} | sort -k1,1 > {}'.format(in_path, job.data_path))


class SubJob:
    """
    Represents a piece of the job that will be sent to a worker

    It supports 'before' and 'after' tasks and can depend on other
    other jobs finishing (see .is_ready_to_execute() for criteria)

    When using a SubJob, pre_execute and post_execute should be run
    before and after the job respectively
    """
    def __init__(self,
                 id,
                 instruction_path,
                 instruction_type,
                 data_path=None,

                 # List of jobs that need the results to continue
                 # Use if other jobs depend on the output of this job
                 pass_result_to=[],

                 # Wait for several data-producing jobs to finish
                 data_paths_list=[],
                 num_data_paths_required=0,

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

        self.pass_result_to = pass_result_to

        self.data_paths_list = data_paths_list
        self.num_data_paths_required = num_data_paths_required

        self.do_before = do_before
        self.do_after = do_after

        self.client = None
        self.pending_assignment = True

    def is_ready_to_execute(self):
        """
        Is this job ready to execute? I.e. is it waiting on data from
        other jobs or not
        :return:
        # """
        # if self.pass_result_to:
        #     return True

        if self.data_paths_list and len(self.data_paths_list) == self.num_data_paths_required:
            return True

        return False

    def is_last(self):
        """
        Is this the last job to be done
        I.e. does it have no dependencies?
        :return:
        """
        return not self.pass_result_to

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

        if self.pass_result_to:
            if type(self.pass_result_to) is list:
                for job in self.pass_result_to:
                    job.data_paths_list.append(output_path)
            else:
                self.pass_result_to.data_paths_list.append(output_path)
