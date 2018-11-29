from pyspark import SparkConf, SparkContext
from pyspark.sql import Row
import sys

conf = SparkConf().setMaster("local").setAppName("cantidad_viajes")
sc = SparkContext(conf=conf)

if len(sys.argv) < 4:
    sys.exit(
        "\nPRIMER PARAMETRO ARCHIVO DE ENTRADA \n"
        + "SEGUNDO PARAMETRO DIRECTORIO DE SALIDA\n"
        + "TERCER PARAMETRO DURACION\n"
    )

arg1 = sys.argv[1]  # file trafico /tmp/data/Entrega2/trafico.txt
arg2 = sys.argv[2]  # salida /tmp/data/Entrega2/05/salida
duracion = int(sys.argv[3])

lines = sc.textFile(arg1)

# (vehiculo, timestamp)
lines = lines.map(lambda line: line.split("\t")).map(lambda x: (x[0], int(x[3])))

#maximo timestamp
max_timestamp = lines.map(lambda a: a[1]).max()

def get_interval(timestamp):
    i = timestamp // duracion
    r = 1 if timestamp % duracion else 0
    return i + r

# obtengo el intervalo al que pertenece el timestamp
#(auto, interv1)
autos_interval = lines.mapValues(get_interval)

#agrupo por intervalo la cantidad de autos distintos contabilizados
#(#intervalo, inicio, fin, cantidad_autos)
intervals = [
    (
        interval,
        (interval - 1) * duracion,
        interval * duracion if (interval * duracion < max_timestamp) else max_timestamp,
        autos_interval.filter(lambda a: a[1] == interval)
        .map(lambda a: (a[0]))
        .distinct()
        .count(),
    )
    for interval in range(1, get_interval(max_timestamp) + 1)
] #para cadda intervalo, calcula cuantos autos sin repetetir hay

sc.parallelize(intervals).saveAsTextFile(arg2)
sc.stop()
