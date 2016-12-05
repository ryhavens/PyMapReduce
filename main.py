from PMRProcessing.mapper.word_count_mapper import Mapper
from PMRProcessing.reducer.word_count_reducer import Reducer
import os

brown = open('brown.txt','r')
f1 = open('f1.txt', 'w')
mapper = Mapper(in_stream=brown, out_stream=f1)
mapper.map()
f1.close()

os.system('cat f1.txt | sort -k1,1 > f2.txt')

f2 = open('f2.txt', 'r')
reducer = Reducer(in_stream=f2, out_stream=open('right.txt', 'w'))
reducer.reduce()
f2.close()
