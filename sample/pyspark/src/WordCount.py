import os
import sys

os.environ['SPARK_HOME'] = "/Users/nandini/Documents/spark/spark-2.3.2-bin-hadoop2.7"
os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.7'

# os.environ['PYSPARK_SUBMIT_ARGS'] = ('--packages com.databricks:csv api pyspark-shell')
sys.path.append('/Users/nandini/Documents/spark/spark-2.3.2-bin-hadoop2.7/python')
sys.path.append('/Users/nandini/Documents/spark/spark-2.3.2-bin-hadoop2.7/python/lib/py4j-0.10.7-src.zip')


from pyspark import SparkConf, SparkContext

sparkConf = SparkConf().setAppName("wordcount").setMaster("local[*]")
sc = SparkContext(conf=sparkConf)

file = sc.textFile(
    "/Users/nandini/eclipse-pydev-workspace/SamplePysparkProject/sample/pyspark/project/resources/wordcount.txt")
output = file.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1)).reduceByKey(lambda x, y: (x + y))
for (word, count) in output.collect():
    print("%s: %i" % (word, count))

sc.stop()
