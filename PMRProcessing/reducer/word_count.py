class Reducer():

    def reduce(self, key, values, output):
        """
        Simple count reduce
        Assumes words sorted by key
        """
        sum = 0
        for value in values:
            sum += int(value)
        output.append((key, sum))