from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
import sys

if len(sys.argv) < 2:
    persistent = "/tmp/data/Entrega3/02/"
else:
    persistent = sys.argv[1]

conf = SparkConf().setMaster("local[2]").setAppName("ContarDestinos")
sc = SparkContext(conf=conf)
sc.setLogLevel("OFF")

ssc = StreamingContext(sc, 5)
stream = ssc.socketTextStream("localhost", 7777)
ssc.checkpoint(persistent + "counts")

# rdd inicial con todos los posibles destinos.
# la usamos para el history.
initialStateRDD = sc.parallelize(
    [
        (place, 0)
        for place in ["Zoologico", "Shopping", "Plaza", "Museo", "Cine", "Teatro"]
    ]
)

counts = (
    stream.map(lambda line: line.split(";"))
    .map(lambda x: (x[4], 1))  # (lugar, 1)
    .filter(
        lambda a: a[0] != "" and a[0] != "Otro"
    )  # filtramos los que no son lugares y Otros
    .reduceByKey(lambda a, b: a + b)  # Sumarizamos
)


def fUpdate(newValues, history):
    if history == None:
        history = 0
    if newValues == None:
        newValues = 0
    else:
        newValues = sum(newValues)  # viene un arreglo con los nuevos valores
    return newValues + history


history = counts.updateStateByKey(fUpdate, initialRDD=initialStateRDD)

# https://spark.apache.org/docs/latest/streaming-programming-guide.html#dataframe-and-sql-operations
# fue la manera que encontramos para mostrar los datos.
# tomamos los 3 lugares más visitados
history.foreachRDD(
    lambda time, rdd: print(
        " {} -- {}".format(time, rdd.takeOrdered(3, key=lambda a: -a[1]))
    )
)

ssc.start()
ssc.awaitTermination()
