from collections import defaultdict
from optparse import OptionParser
import os

def parse_opts():
        parser = OptionParser()
        parser.add_option('-d', '--datafile', dest='datafile',
                          help='file to check against', type='string', default='right.txt')
        return parser.parse_args()

options, args = parse_opts()

print('Checking files in output against canonical file %s' % options.datafile)

right = open(options.datafile,'r')
outfile = open('compiled_output.txt','w')
vocabulary = defaultdict(lambda: 0)

for line in right:
    line = line.strip().split('\t')
    vocabulary[line[0]] = int(line[1])

out_count = 0
for filename in os.listdir('output'):
    with open('output/' + filename,'r') as f:
        for line in f:
            outfile.write(line)
            out_count += 1
            line = line.strip().split('\t')
            if not vocabulary[line[0]]:
                print('%s was not found in canonical text')
            elif (vocabulary[line[0]] != int(line[1])):
                print('%s: Expected %d, found %d' % (line[0], vocabulary[line[0]], int(line[1])))

print('%d words in canonical, %d words in output' % (len(vocabulary.keys()), out_count))
print('Writing compiled output to compiled_output.txt')
print('Done')

