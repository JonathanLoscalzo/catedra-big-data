from pyspark import SparkConf, SparkContext
import sys 

arg1 = sys.argv[1] 
arg2 = sys.argv[2]

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

text_file = sc.textFile(arg1)
counts = text_file.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)
counts.saveAsTextFile(arg2)