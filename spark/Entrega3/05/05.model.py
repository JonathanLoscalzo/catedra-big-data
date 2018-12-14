from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext

import sys

if len(sys.argv) < 3:
    persistent = "/tmp/data/Entrega3/05/"
    window = 15
    saveFile = False
else:
    persistent = sys.argv[1]
    window = int(sys.argv[2])
    saveFile = True


def fUpdate(newValues, history):
    return (history or 0) + sum((newValues or [0]))


conf = SparkConf().setMaster("local[2]").setAppName("ALS - generacion modelo")
sc = SparkContext(conf=conf)
sc.setLogLevel("OFF")

ssc = StreamingContext(sc, window)

initial = sc.parallelize([])
stream = ssc.socketTextStream("localhost", 7777)

ssc.checkpoint(persistent + "als")

counts = (
    stream.map(lambda line: line.split(";"))
    .map(lambda x: ((int(x[0]), x[4]), 1))  # ((vehiculo, destino), 1)
    .filter(lambda a: a[0][1] != "")
    .reduceByKey(lambda a, b: a + b)  # ((vehiculo, destino), ranking)
)

history = counts.updateStateByKey(fUpdate, initialRDD=initial)
history.pprint()

def toTabularLine(data):
    return '\t'.join(str(d) for d in data)

if saveFile:
    #(vehiculo, destino, ranking)
    history.map(lambda a: (a[0][0], a[0][1], a[1])).map(toTabularLine).saveAsTextFiles(
        persistent + "trafico-stream"
    )

ssc.start()
ssc.awaitTermination()
