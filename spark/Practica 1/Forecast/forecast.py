# Los archivos estacionSur.txt y estacionNorte.txt tienen información
# sobre datos climáticos (temperatura, humedad y precipitación).

from pyspark import SparkConf, SparkContext
import sys

conf = SparkConf().setMaster("local").setAppName("Sales")
sc = SparkContext(conf=conf)


def fahrenheitToCelcius(farenheit):
    return (farenheit - 32) * 5 // 9


def cmToMm(cm):
    return cm * 10


# estacionNorte.txt almacena la información en grados centígrados,
# porcentaje de humedad, y milímetros de lluvia.
fileNorte = sc.textFile("Estaciones/estacionNorte.txt")

# estacionSur.txt almacena la información en grados fahrenheit, porcentaje de
# humedad, y centímetros de lluvia.
fileSur = sc.textFile("Estaciones/estacionSur.txt")

lines_sur = fileSur.map(lambda line: line.split("\t")).map(
    lambda a: (a[0], fahrenheitToCelcius(int(a[1])), int(a[2]), cmToMm(int(a[3])))
)

lines_norte = fileNorte.map(lambda line: line.split("\t")).map(
    lambda a: (a[0], int(a[1]), int(a[2]), int(a[3]))
)

lines = lines_norte.union(lines_sur)

# Arme una aplicación en Spark que obtenga el promedio de
# temperatura, de humedad y precipitación total entre todas las
# estaciones.

# haciendo las cuentas a mano fue más rapido
averages = (
    lines.map(lambda a: (a[0], (a[1], a[2], a[3], 1)))
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2], a[3] + b[3]))
    .map(lambda x: (x[0], x[1][0] / x[1][3], x[1][1] / x[1][3], x[1][2] / x[1][3]))
)
# cambiar reduceBykey por fold(zeroValue, op) o foldByKey


seqOp = lambda agg, line: (
    agg[0] + line[0],
    agg[1] + line[1],
    agg[2] + line[2],
    agg[3] + 1,
)
combOp = lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2], x[3] + y[3])

# usando aggregates.
averages2 = (
    lines.map(lambda a: (a[0], (a[1], a[2], a[3], 1)))
    .aggregateByKey((0, 0, 0, 0), seqOp, combOp)
    .map(lambda x: (x[0], x[1][0] / x[1][3], x[1][1] / x[1][3], x[1][2] / x[1][3]))
)

averages.saveAsTextFile("averages")

# Además deberá informar la estación más fría, la más
# calurosa, la más húmeda, la menos húmeda, la más lluviosa y la
# menos lluviosa.
estaciones_temperatura = lines.map(lambda x: (x[0], x[1]))
estaciones_humedad = lines.map(lambda x: (x[0], x[2]))
estaciones_lluvia = lines.map(lambda x: (x[0], x[3]))

getMost = lambda a, b: max(a, b, key=lambda x: x[1])
getLeast = lambda a, b: min(a, b, key=lambda x: x[1])

print(
    "mas temperatura {} \
    menos temperatura {} \
    mas lluviosa {} \
    menos lluviosa {} \
    mas humedad {} \
    menos humedad {} \
    ".format(
        estaciones_temperatura.reduce(getMost),
        estaciones_temperatura.reduce(getLeast),
        estaciones_humedad.reduce(getMost),
        estaciones_humedad.reduce(getLeast),
        estaciones_lluvia.reduce(getMost),
        estaciones_lluvia.reduce(getLeast),
    )
)