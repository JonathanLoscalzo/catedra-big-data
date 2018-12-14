from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
import sys

if len(sys.argv) < 2:
    persistent = "/tmp/data/Entrega3/03/"
else:
    persistent = sys.argv[1]

conf = SparkConf().setMaster("local[2]").setAppName("ContarCoordenadas")
sc = SparkContext(conf=conf)
sc.setLogLevel("OFF")

ssc = StreamingContext(sc, 5)
stream = ssc.socketTextStream("localhost", 7777)
ssc.checkpoint(persistent + "counts")

counts = (
    stream.map(lambda line: line.split(";"))
    .map(lambda x: ((int(x[1]), int(x[2])), x[4]))
    .filter(lambda a: a[1] != "")
    .mapValues(lambda a: 1)
    .reduceByKey(lambda a, b: a + b)
)


def fUpdate(newValues, history):
    if history == None:
        history = 0
    if newValues == None:
        newValues = 0
    else:
        newValues = sum(newValues)  # viene un arreglo con los nuevos valores
    return newValues + history


history = counts.updateStateByKey(fUpdate)

# https://spark.apache.org/docs/latest/streaming-programming-guide.html#dataframe-and-sql-operations
history.foreachRDD(
    lambda time, rdd: print(
        " {} -- {}".format(time, rdd.takeOrdered(10, key=lambda a: -a[1]))
    )
)

ssc.start()
ssc.awaitTermination()
