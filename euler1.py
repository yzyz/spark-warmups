from pyspark import SparkContext
from operator import add

sc = SparkContext("local", "Project Euler Problem 1")

a = sc.parallelize(range(1, 1000)) \
      .filter(lambda x: x % 3 == 0 or x % 5 == 0) \
      .reduce(add)

print(a)
