import os

from PMRJob.sub_job import *
from filesystems import SimpleFileSystem


def hashcode(s):
    """
    Return the hashcode of a string

    https://gist.github.com/hanleybrand/5224673
    :param s:
    :return: int
    """
    h = 0
    for c in s:
        h = (31 * h + ord(c)) & 0xFFFFFFFF
    return ((h + 0x80000000) & 0xFFFFFFFF) - 0x80000000


def setup_mapping_tasks(data_path, mapper_name, num_workers, sub_jobs, get_next_job_id):
    # partitions = chunk_input_data(data_path)
    partitions = chunk_input_data_by_size_and_workers(data_path, num_workers)

    for index, partition_path in enumerate(partitions):
        sub_jobs.append(SubJob(
            id=get_next_job_id(),
            instruction_path=mapper_name,
            num_workers=num_workers,
            instruction_type='Mapper',
            data_path=partition_path,
            do_after=[set_result_file]
        ))


def setup_reducing_tasks(reducer_name, num_reducers, sub_jobs, get_next_job_id):
    # For each partition, create a reducer job
    for index in range(num_reducers):
        sub_jobs.append(SubJob(
            id=get_next_job_id(),
            instruction_path=reducer_name,
            num_workers=num_reducers,
            instruction_type='Reducer',
            partition_num=index,
            do_after=[set_result_file]
        ))


def get_job_result_file_path(num_partitions):
    # For demo and testing purposes, we combine and sort final partitions here.
    # Note: this negates the distributed advantages so remove for real work
    sf = SimpleFileSystem()
    path = sf.get_writeable_file_path()
    path_sorted = sf.get_writeable_file_path()
    f = sf.open(path, 'w')

    for i in range(num_partitions):
        pf = sf.open(sf.get_file_with_name('partition_{}'.format(i)), 'r')
        f.write(pf.read())
        pf.close()

    sf.close(f)

    os.system('cat {} | sort -k1,1 > {}'.format(path, path_sorted))

    return path_sorted
