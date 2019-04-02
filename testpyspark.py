#!/usr/bin/env python
# -*- coding:utf8 -*-
from pyspark import SparkContext
logFile = '/user/mafei/filecount.txt'
sc = SparkContext('local', 'Simple App')
logDATA = sc.textFile(logFile).cache()
numAs = logDATA.filter(lambda s: 'mafei' in s).count()
numBs = logDATA.filter(lambda s: 'mass' in s).count()
numCs = logDATA.filter(lambda s: 'majiuyu' in s).count()

print(numAs, numBs, numCs)

aa = sc.parallelize(list((3, 1, 2, 5, 5)))
print(aa.collect())
