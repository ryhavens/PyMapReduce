from abc import ABCMeta, abstractmethod


class PMRJob(metaclass=ABCMeta):
    """
    The abstract class for jobs that will be
    run on clients
    """

    @abstractmethod
    def run(self):
        """
        Run the job
        :return:
        """
        pass
