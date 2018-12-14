from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
import sys

if len(sys.argv) < 3:
    persistent = "/tmp/data/Entrega3/04/"
    duracion = 100
else:
    persistent = sys.argv[1]
    duracion = sys.argv[2]


def get_interval(timestamp):
    """
    me devuelve a que intervalo pertenece el timestamp
    """
    i = timestamp // duracion
    r = 1 if timestamp % duracion else 0
    return i + r


def as_tuple_range(interval):
    """
    Retorna una tupla correspondiente al intervalo => (ini, fin)
    """
    fin = interval * duracion
    return (fin - duracion, fin)


def fUpdate(newValues, history):
    return set((history or [])).union(newValues)


conf = SparkConf().setMaster("local[2]").setAppName("ContarCoordenadas")
sc = SparkContext(conf=conf)
sc.setLogLevel("OFF")
ssc = StreamingContext(sc, 5)

initial = sc.parallelize([])
stream = ssc.socketTextStream("localhost", 7777)
ssc.checkpoint(persistent + "counts")

counts = stream.map(lambda line: line.split(";")).map(
    lambda x: (get_interval(int(x[3])), x[0])
)

history = counts.updateStateByKey(fUpdate, initialRDD=initial)

# https://spark.apache.org/docs/latest/streaming-programming-guide.html#dataframe-and-sql-operations
history.foreachRDD(
    lambda time, rdd: print(
        "  duracion {} -- (intervalo - autos) {}".format(
            duracion,
            rdd.mapValues(lambda a: len(a))
            .sortByKey(False)
            .map(lambda a: (as_tuple_range(a[0]), a[1]))
            .collect(),
        )
    )
)

ssc.start()
ssc.awaitTermination()
