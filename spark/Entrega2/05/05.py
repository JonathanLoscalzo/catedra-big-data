from pyspark import SparkConf, SparkContext
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

def get_interval(timestamp):
    """
    me devuelve a que intervalo pertenece el timestamp

    2999 // 3000 => i = 0
    + (2999  % 3000 == 0) => no => 0 
    Final = 2999 está en el 0

    3100 // 3000 => i = 1
    + (3100 % 3000 == 0) => No => 0
    Final 3100 está en el 1

    6000 // 3000 => i = 2
    + (6000 % 3000 == 0 ) => si => 1
    Final: 6000 está en el 3
    """
    i = timestamp // duracion # 
    r = 1 if timestamp % duracion else 0
    return i + r

# obtengo el intervalo al que pertenece el timestamp
# (auto, interv1)
autos_interval = lines.mapValues(get_interval)

# me quedo con los no repetidos, los invierto, y cuento por key
intervals = (
    autos_interval.distinct().map(lambda a: (a[1], 1)).reduceByKey(lambda a, b: a + b).sortByKey()
)

intervals.saveAsTextFile(arg2)
sc.stop()
