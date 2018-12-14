from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
import sys

persistent = sys.argv[1] if (len(sys.argv) >= 2) else "/tmp/data/Entrega3/05/entrada"
car = int(sys.argv[2]) if (len(sys.argv) >= 3) else 10


def fUpdate(newValues, history):
    return (history or 0) + sum((newValues or [0]))


conf = SparkConf().setMaster("local[2]").setAppName("ALS - prediccion modelo")
sc = SparkContext(conf=conf)
sc.setLogLevel("OFF")

PLACES = ["Zoologico", "Shopping", "Plaza", "Museo", "Cine", "Teatro", "Otro"]

err = 0.1
#matrices de autos (U, FU)
u_matrix = sc.parallelize([(i, 1) for i in range(1, 21)])

#matrices de lugares (T, FT)
t_matrix = sc.parallelize([(i, 1) for i in PLACES])

lines = sc.textFile(persistent).map(lambda a: a.split("\t"))

# ((vehiculo, lugar), ranking)
# ((U, T), R)
rankings = lines.map(lambda a: ((int(a[0]), a[1]), int(a[2]))).reduceByKey(
    lambda a, b: a + b
)

def calculate_new_matrix(matrix_f, rankings_mapped):
    """
    matrix_f es la matrix de coeficientes
        (U, FU) o (T, FT)
    rankings_mapped son los rankings con este formato 
        (Vehiculo, (Lugar, Ranking)) or (lugar, (vehiculo, Ranking))
        (U, (T, R)) o (T, (U, R))
        
    """
    # (U, FU) x (U, (T, R)) => (U, (FU, (T, R)))
    #  o
    # (T, FT) x (T, (U, R)) => (T, (FT, (U, R)))
    news = matrix_f.join(rankings_mapped)

    # (U, (FT, R)) o (T, (FU, R))
    news = news.map(lambda a: (a[1][1][0], (a[1][0], a[1][1][1])))

    # (U, (FT**2, FT*R)
    news = news.map(lambda a: (a[0], (a[1][0] ** 2, a[1][1] * a[1][0])))

    # (U, (SUM(FT**2), SUM(FT*R))
    news = news.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

    # (U, (1/(SUM(FT**2)+err) * sum(FT * R) ))
    news = news.mapValues(lambda a: ((1 / (a[0] + err)) * a[1]))
    return news

i = 0

while i < 10:
    i += 1
    # T
    # U matrix = (U, FU)
    # RANKINGS = (U, (V, R))
    t_news = calculate_new_matrix(
        u_matrix, rankings.map(lambda a: (a[0][0], (a[0][1], a[1])))
    )

    # U
    # T matrix = (T, FT)
    # RANKINGS = (T, (U, R))
    u_news = calculate_new_matrix(
        t_matrix, rankings.map(lambda a: (a[0][1], (a[0][0], a[1])))
    )

    t_matrix = t_news
    u_matrix = u_news

# t_matrix.saveAsTextFile(persistent + "matrix/t")
# u_matrix.saveAsTextFile(persistent + "matrix/u")

prediction = (
    u_matrix.cartesian(t_matrix).map(
        lambda a: ((a[0][0], a[1][0]), (a[0][1] * a[1][1]))
    )
)

def printable(rdd):
    return (rdd
        .filter(lambda a: a[0][0] == car)
        .map(lambda a: (a[1], a[0]))
        .sortByKey(False)
        .map(lambda a: (a[1][1], a[0]))
        .take(10)
    )

print(
    "VEHICULO {}\n\n - PREDICCIÓN {} \n\n- Linea Matriz {}\n\n".format(
        car,
        printable(prediction),
        printable(rankings)
    )
)

