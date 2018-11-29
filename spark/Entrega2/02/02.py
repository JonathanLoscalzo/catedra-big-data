from pyspark import SparkConf, SparkContext
import sys

conf = SparkConf().setMaster("local").setAppName("cantidad_viajes")
sc = SparkContext(conf=conf)

if (len(sys.argv) < 3):
    sys.exit("\n\nPRIMER PARAMETRO ARCHIVO DE ENTRADA"+
    "SEGUNDO PARAMETRO DIRECTORIO DE SALIDA\n\n")

arg1 = sys.argv[1]  # file trafico /tmp/data/Entrega2/trafico.txt
arg2 = sys.argv[2]  # salida /tmp/data/Entrega2/02/salida

lines = sc.textFile(arg1)
lines = lines.map(lambda line: line.split("\t")).map(lambda x: (x[4], 1))

# Manera 1
by_dest = lines.filter(lambda a: a[0] != "" and a[0] != "Otro").countByKey().items()
by_dest = list(by_dest)
by_dest.sort(key=lambda a: a[1], reverse=True)
sc.parallelize(by_dest[0:3]).saveAsTextFile(arg2) # no es Big Data

# Manera 2
by_dest_d = (
    lines.filter(lambda a: a[0] != "" and a[0] != "Otro")
    .reduceByKey(lambda a, b: a + b) #foldByKey(0, sum)
    .takeOrdered(3, key=lambda a: -a[1])
)
sc.parallelize(by_dest_d).saveAsTextFile(arg2+"2") # no es Big Data

