from pyspark import SparkConf, SparkContext
import sys

conf = SparkConf().setMaster("local").setAppName("cantidad_viajes")
sc = SparkContext(conf=conf)

if len(sys.argv) < 3:
    sys.exit(
        "\n\nPRIMER PARAMETRO ARCHIVO DE ENTRADA\n"
        + "SEGUNDO PARAMETRO DIRECTORIO DE SALIDA\n\n"
    )

arg1 = sys.argv[1]  # file trafico /tmp/data/Entrega2/trafico.txt
arg2 = sys.argv[2]  # salida /tmp/data/Entrega2/01/salida

# cuantos viajes realizó cada vehículo.
lines = sc.textFile(arg1)

# (vehiculo, lugar)
lines = lines.map(lambda line: line.split("\t")).map(lambda x: (int(x[0]), x[4]))

# MANERA 1
ej1 = lines.filter(lambda a: a[1] != "").countByKey().items()
sc.parallelize(ej1).saveAsTextFile(arg2)

# MANERA 2
seqOp = lambda x, y: (x + 1)
combOp = lambda x, y: (x + y)
ej1_rdd = lines.filter(lambda a: a[1] != "").aggregateByKey((0), seqOp, combOp)
ej1_rdd.saveAsTextFile(arg2 + "2")

sc.stop()
