from PMRProcessing.mapper.mapper import Mapper
from PMRProcessing.reducer.reducer import Reducer
import os

brown = open('brown.txt','r')
f1 = open('f1.txt', 'w')
mapper = Mapper(instream=brown, outstream=f1)
mapper.Map()
f1.close()

os.system('cat f1.txt | sort -k1,1 > f2.txt')

f2 = open('f2.txt', 'r')
reducer = Reducer(instream=f2, outstream=open('/dev/null','w'))
reducer.Reduce()
f2.close()
