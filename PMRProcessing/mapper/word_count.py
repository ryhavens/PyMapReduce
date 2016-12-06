class Mapper():

    def map(self, key, value, output):
        """

        :param key: file name
        :param value: line
        :param output: what to append result pairs too
        :return:
        """
        value = value.strip()
        words = value.split()
        for word in words:
            output.append((word, 1))