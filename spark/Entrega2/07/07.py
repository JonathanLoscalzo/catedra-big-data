from pyspark import SparkConf, SparkContext
from math import sqrt
import sys

conf = SparkConf().setMaster("local").setAppName("cantidad_viajes")
sc = SparkContext(conf=conf)

if len(sys.argv) < 2:
    sys.exit(
        "\nPRIMER PARAMETRO ARCHIVO DE ENTRADA \n"
        + "SEGUNDO PARAMETRO DIRECTORIO DE SALIDA\n"
    )

arg1 = sys.argv[1]  # file trafico /tmp/data/Entrega2/trafico.txt
k = 5

lines = sc.textFile(arg1)

lines = lines.map(lambda line: line.split("\t"))

#((lat, long), auto)
coord_autos = lines.map(lambda x: (int(x[1]), int(x[2]), x[0]))

# ((lat, log)
lines = lines.map(lambda x: (int(x[1]), int(x[2])))

# (x,y)
centroides = lines.takeSample(False, k)

def distancia_puntos(c, p):
    """
    Calcula la distancia entre 2 puntos
    """
    return sqrt((c[0] - p[0]) ** 2 + (c[1] - p[1]) ** 2)


def closestPoint(p, centroides):
    """
        Retorna el indice del centroide más cercano
    """
    closest = float("inf")
    index = None
    for (i, c) in enumerate(centroides):
        aux = distancia_puntos(c, p)
        if closest > aux:
            closest = aux
            index = i
    return index


i = 0
error = 1
convergencia = float("inf")

while i < 10 and convergencia > error:
    i += 1
    
    # (index_centroide, x,y,1)
    closest = lines.map(lambda p: (closestPoint(p, centroides), (p[0], p[1], 1)))

    # por centroide calculo la sumatoria para luego obtener el promedio
    sumatoria = closest.reduceByKey(
        lambda p1, p2: (p1[0] + p2[0], p1[1] + p2[1], p1[2] + p2[2])
    )

    # obtengo los nuevos centroices promedios
    nuevos_puntos = sumatoria.map(
        lambda p: (p[0], (p[1][0] / p[1][2], p[1][1] / p[1][2]))
    ).collect()

    # WIKI: El algoritmo se considera que ha convergido cuando las asignaciones ya no cambian
    convergencia = sum(distancia_puntos(centroides[i], p) for (i, p) in nuevos_puntos)

    # actualizo centroides
    for (c, p) in nuevos_puntos:
        centroides[c] = p

# Cuando termino, calculo el centroide más cercado por punto-auto. Luego me quedo con los distintos #(centro, auto) y los cuento por centroide
centroides_autos = coord_autos.map(lambda p: (centroides[closestPoint(p, centroides)], p[2])).distinct().countByKey()

print(
    "___________________________________________________________________"
    "\n _________ FIN DE EJECUCIÓN __________\n"
    + "____________ Iteraciones     = {}__________\n".format(i)
    + "____________ Convergencia    = {}__________\n".format(convergencia)
    + "____________ Centroides      = {}\n\n".format(centroides)
    + "_____ Cant autos x centroide = {}\n".format(dict(centroides_autos))
    + "\n______________________________________________________________\n"
)
