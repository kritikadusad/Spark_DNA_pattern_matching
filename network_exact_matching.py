#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from operator import add
from datetime import datetime
from pyspark.sql.functions import collect_list


# In[2]:


time_start = datetime.now()
sc = SparkContext("local", "exactmatching")
sqlContext = SQLContext(sc)


# In[3]:


def expand_line(tup, length):
    file_id = tup[0]
    text = tup[1]
    results = []
    for i in range(len(text) - length + 1):
        tup = (file_id, i, text[i : i + length])
        results.append(tup)
    return results

def position(tup, pattern):
    if tup[2] == pattern:
        return (tup[0], tup[1])

   


# In[4]:


genomes = sc.wholeTextFiles("file:///home/krtika/*.fa")
genomes.getNumPartitions()


# In[5]:


def remove_first_line(text):
    first_newline_index = text[1].index("\n")
    return (text[0], text[1][first_newline_index + 1:])
genomes = genomes.map(remove_first_line)
# genomes.take(1)


# In[6]:

genomes = genomes.map(lambda tup: (tup[0], tup[1].replace("\n", "")))
# genomes.take(1)


# In[7]:

pattern = 'GGGC'
length = len(pattern)
count = genomes.flatMap(lambda x: expand_line(x, length))     .map(lambda tup: position(tup, pattern))     .filter(lambda pos: pos if pos != None else "") 

time_end = datetime.now()
print(time_end-time_start)


df = sqlContext.createDataFrame(count, ['file name', 'position'])

grouped_df = df.groupby('file name').agg(collect_list('position').alias('position'))
grouped_df.show()

sc.stop()

