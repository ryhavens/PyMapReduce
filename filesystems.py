import os
import random
import string
import pathlib
from abc import ABCMeta, abstractmethod


class BaseFilesystem(metaclass=ABCMeta):
    """
    An abstract base class for abstracting away filesystems
    """
    @abstractmethod
    def get_writeable_file_path(self):
        pass

    @abstractmethod
    def open(self, path, mode):
        pass

    @abstractmethod
    def close(self, file):
        pass


class SimpleFileSystem(BaseFilesystem):
    def __init__(self, fs_base_path='temp', output_base_path='output'):
        self._fs_base_path = fs_base_path
        self._output_base_path = output_base_path
        self._file_name_len = 10

        if not os.path.exists(self._fs_base_path):
            os.makedirs(self._fs_base_path)

        if not os.path.exists(self._output_base_path):
            os.makedirs(self._output_base_path)

    def get_writeable_file_path(self):
        f_name = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(self._file_name_len))
        return '{path}.txt'.format(path=os.path.join(self._fs_base_path, f_name))

    def get_file_with_name(self, name):
        return '{path}.txt'.format(path=os.path.join(self._fs_base_path, name))

    def get_mapper_output_file(self, partition_num):
        dir_name = 'partition_{}'.format(partition_num)
        file = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(self._file_name_len))\
               + '.txt'

        if not os.path.exists(os.path.join(self._fs_base_path, dir_name)):
            os.makedirs(os.path.join(self._fs_base_path, dir_name))
        return os.path.join(self._fs_base_path, dir_name, file)

    def get_partition_files(self, partition_num):
        dir_name = 'partition_{}'.format(partition_num)
        path = os.path.join(self._fs_base_path, dir_name)
        return [os.path.join(path, f) for f in os.listdir(path)]

    def get_output_file(self, partition_num):
        f_name = 'output_part_{}.txt'.format(partition_num)
        return os.path.join(self._output_base_path, f_name)

    def _delete_folder(self, path):
        if type(path) is str:
            path = pathlib.Path(path)
        for sub in path.iterdir():
            if sub.is_dir():
                self._delete_folder(sub)
            else:
                sub.unlink()

    def clean_directories(self):
        self._delete_folder(self._fs_base_path)
        self._delete_folder(self._output_base_path)

    def open(self, path, mode):
        return open(path, mode)

    def close(self, file):
        file.close()
