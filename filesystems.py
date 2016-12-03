import os
import random
import string
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
    def __init__(self, fs_base_path='temp'):
        self._fs_base_path = fs_base_path
        self._file_name_len = 10

        if not os.path.exists(self._fs_base_path):
            os.makedirs(self._fs_base_path)

    def get_writeable_file_path(self):
        f_name = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(self._file_name_len))
        return '{path}.txt'.format(path=os.path.join(self._fs_base_path, f_name))

    def open(self, path, mode):
        return open(path, mode)

    def close(self, file):
        file.close()
