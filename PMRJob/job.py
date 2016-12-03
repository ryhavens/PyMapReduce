from PMRJob.sub_job import fixed_size_partition, SubJob, stitch_data_files, sort_data_file


def prep_job_for_execution(data_path, reducer_name, mapper_name,
                           pending_jobs, sub_jobs, get_next_job_id):
    """
    Given a data_path, mapper_name and reducer_name, prep the job for
    distributed execution

    :param data_path: Path to data file
    :param reducer_name: Path to reducer module
    :param mapper_name: Path to mapper module
    :param pending_jobs: server's list of pending jobs
    :param sub_jobs: server's list of sub_jobs
    :param get_next_job_id: server's handle for getting next job id
    :return:
    """
    partitions = fixed_size_partition(data_path)

    final_reduce = SubJob(id=get_next_job_id(),
                          instruction_path=reducer_name,
                          instruction_type='Reducer',
                          data_paths_list=[],
                          num_data_paths_required=len(partitions),
                          # Stitch files together and sort output before start reducer
                          do_before=[stitch_data_files, sort_data_file])

    for i in range(len(partitions)):
        pending_jobs.append(SubJob(
            id=get_next_job_id(),
            instruction_path=reducer_name,
            instruction_type='Reducer',
            data_paths_list=[],
            num_data_paths_required=1,
            # Stitch even though its one file bc stitch also sets the file up for processing
            do_before=[stitch_data_files, sort_data_file],
            pass_result_to=final_reduce
        ))

    pending_jobs.append(final_reduce)

    for index, partition_path in enumerate(partitions):
        sub_jobs.append(SubJob(
            id=get_next_job_id(),
            instruction_path=mapper_name,
            instruction_type='Mapper',
            data_path=partition_path,
            pass_result_to=pending_jobs[index]
        ))