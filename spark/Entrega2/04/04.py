from pyspark import SparkConf, SparkContext
from pyspark.sql import Row
import sys

conf = SparkConf().setMaster("local").setAppName("cantidad_viajes")
sc = SparkContext(conf=conf)

if len(sys.argv) < 5:
    sys.exit(
        "\nPRIMER PARAMETRO ARCHIVO DE ENTRADA \n"
        + "SEGUNDO PARAMETRO DIRECTORIO DE SALIDA\n"
        + "TERCER PARAMETRO TIMESTAMP INICIO\n"
        + "CUARTO PARAMETRO TIMESTAMP FIN\n"
    )

arg1 = sys.argv[1]  # file trafico /tmp/data/Entrega2/trafico.txt
arg2 = sys.argv[2]  # salida /tmp/data/Entrega2/04/salida
ini = int(sys.argv[3])
fin = int(sys.argv[4])
if ini > fin:
    sys.exit(
        "\nINICIO NO PUEDE SER MAYOR A FIN: {ini} - {fin}\n\n".format(ini=ini, fin=fin)
    )

lines = sc.textFile(arg1)

# (vehiculo, timestamp)
lines = lines.map(lambda line: line.split("\t")).map(lambda x: (x[0], int(x[3])))

# MANERA 1 - poco bigdata
total_autos = len(lines.filter(lambda a: ini <= a[1] <= fin).countByKey().keys())

# MANERA 2 - con big data, pero groupby es ineficiente
total_autos_2 = lines.filter(lambda a: ini <= a[1] <= fin).groupByKey().count()

# MANERA 3
total_autos_3 = lines.filter(lambda a: ini <= a[1] <= fin).map(lambda a: a[0]).distinct().count()

sc.parallelize([total_autos]).saveAsTextFile(arg2)
sc.parallelize([total_autos_2]).saveAsTextFile(arg2 + "2")
sc.parallelize([total_autos_3]).saveAsTextFile(arg2 + "3")
sc.stop()
