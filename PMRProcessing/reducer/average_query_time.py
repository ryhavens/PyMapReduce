class Reducer():

    def reduce(self, key, values, output):
        """
        For the given key, average the values
        :param key:
        :param values:
        :param output:
        :return:
        """
        count = 0
        sum = 0
        for value in values:
            sum += float(value)
            count += 1

        output.append((key, sum/count))
