class Mapper():

    def map(self, key, value, output):
        """
        Convert a db log record to just a time associated with the overall key
        :param key:
        :param value:
        :param output:
        :return:
        """
        values = value.split()
        time = float(values[1][1:-1])  # '(0.000)' --> 0.000

        output.append(('avg', time))
