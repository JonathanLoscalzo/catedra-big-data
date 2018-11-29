from pyspark import SparkConf, SparkContext
import sys

conf = SparkConf().setMaster("local").setAppName("cantidad_viajes")
sc = SparkContext(conf=conf)

if (len(sys.argv) < 3):
    sys.exit("\n\nPRIMER PARAMETRO ARCHIVO DE ENTRADA"+
    "SEGUNDO PARAMETRO DIRECTORIO DE SALIDA\n\n")

arg1 = sys.argv[1]  # file trafico /tmp/data/Entrega2/trafico.txt
arg2 = sys.argv[2]  # salida /tmp/data/Entrega2/03/salida

lines = sc.textFile(arg1)

#((lat, log), lugar)
lines = lines.map(lambda line: line.split("\t")).map(lambda x: ((int(x[1]),int(x[2])), x[4]))

by_dest_d = (
    lines.filter(lambda a: a[1] != "")
    .map(lambda a: (a[0], 1)).reduceByKey(lambda a, b: a + b) #foldByKey(0, sum)
    .takeOrdered(10, key=lambda a: -a[1])
)

sc.parallelize(by_dest_d).saveAsTextFile(arg2) # no es Big Data
