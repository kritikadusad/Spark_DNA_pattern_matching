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


# # To read Fasta file.
# genomes = sc.textFile("file:///home/krtika/*.fa")
# # genome_n = genomes.map(lambda line: line if line[0]=='>' else "")
# # genome_n.distinct().take(2)
# # To save the identifier/name of the genome:
# # genome_id = genomes.map(lambda genome: identifier(genome))
# genome_name = genome_id.filter(lambda name: name[0]==">").take(1)[0]
# # To remove identifier/name from the genome
# genome_id = genomes.first()
# header = sc.parallelize([genome_id])


# In[ ]:





# In[8]:


pattern = 'GGGC'
length = len(pattern)
count = genomes.flatMap(lambda x: expand_line(x, length))     .map(lambda tup: position(tup, pattern))     .filter(lambda pos: pos if pos != None else "") 
# count.take(10)
# time_end = datetime.now()
# print(time_end-time_start)


# In[9]:


df = sqlContext.createDataFrame(count, ['file name', 'position'])


# In[ ]:





# In[10]:


grouped_df = df.groupby('file name').agg(collect_list('position').alias('position'))
grouped_df.show()


# In[11]:


sc.stop()


# In[ ]:




