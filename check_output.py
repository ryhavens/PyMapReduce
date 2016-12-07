from collections import defaultdict
import os

right = open('right.txt','r')
vocabulary = defaultdict(lambda: 0)

for line in right:
    line = line.strip().split('\t')
    vocabulary[line[0]] = int(line[1])

out_count = 0
for filename in os.listdir('output'):
    with open('output/' + filename,'r') as f:
        for line in f:
            out_count += 1
            line = line.strip().split('\t')
            if not vocabulary[line[0]]:
                print("%s was not found in canonical text")
            elif (vocabulary[line[0]] != int(line[1])):
                print("%s: Expected %d, found %d" % (line[0], vocabulary[line[0]], line[1]))

print("%d words in canonical, %d words in output" % (len(vocabulary.keys()), out_count))
print("Done")

