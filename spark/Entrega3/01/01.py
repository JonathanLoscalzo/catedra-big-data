from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
import sys

if len(sys.argv) < 2:
    persistent = "/tmp/data/Entrega3/01/"
else:
    persistent = sys.argv[1]

conf = SparkConf().setMaster("local[2]").setAppName("ContarAutosConDestino")
sc = SparkContext(conf=conf)
sc.setLogLevel("OFF")

ssc = StreamingContext(sc, 5)
stream = ssc.socketTextStream("localhost", 7777)
ssc.checkpoint(persistent + "counts")

# conozco la cantidad de autos de antemano de 0 a 20. le asigno 0 viajes
initialStateRDD = sc.parallelize([(car, 0) for car in range(0, 21)])

counts = (
    stream.map(lambda line: line.split(";"))
    .map(lambda x: (int(x[0]), x[4])) #(vehiculo, lugar)
    .filter(lambda a: a[1] != "") # filtro los que no son destinos
    .mapValues(lambda a: 1) # mapeo cada valor con un 1
    .reduceByKey(lambda a, b: a + b) #hago una reducción para sumar los items
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

history.pprint(21) # pongo 21, para que imprima todos los autos. Default 10

ssc.start()
ssc.awaitTermination()
