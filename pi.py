import sys
from pyspark import SparkContext
from pyspark.mllib.random import RandomRDDs
from math import hypot

def dist(p):
    return hypot(p[0] - 0.5, p[1] - 0.5)

sc = SparkContext("local", "Monte Carlo Integration Pi Approximation")

num_samples = int(sys.argv[1])

a = RandomRDDs.uniformVectorRDD(sc, num_samples, 2)
num = a.map(dist).filter(lambda d: d < 0.5).count()

print(4 * num / num_samples)
