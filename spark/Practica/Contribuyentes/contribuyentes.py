# El departamento fiscal de la ciudad de La Plata almacena para sus
# contribuyentes el monto de diferentes impuestos discriminados por mes y año.

from pyspark import SparkConf, SparkContext
import sys

conf = SparkConf().setMaster("local").setAppName("Sales")
sc = SparkContext(conf=conf)

# - Resulta de interés comparar para un mismo contribuyente y en un
# determinado mes dos períodos históricos diferentes ("anterior al 2000" y
# "después del 2000").
# CONTRIBUYENTE, AÑO, MES, MONTO
lines = sc.textFile("Impuestos.txt")
lines = lines.map(lambda line: line.split("\t")).map(
    lambda x: (int(x[0]), int(x[1]), int(x[2]), float(x[3]))
)

withoutYear = lambda a: ((a[0], a[2]), a[3])
add = lambda a, b: (a + b)

before_2000 = (
    lines.filter(lambda x: x[1] < 2000).map(withoutYear).foldByKey(0, add).sortByKey()
)
after_2000 = (
    lines.filter(lambda x: x[1] >= 2000).map(withoutYear).foldByKey(0, add).sortByKey()
)

# - Se define como "balance M positivo" si para un contribuyente el monto total de
# impuestos pagados en el mes M es mayor en el período "después del 2000"
# que en el período "anterior al 2000". Se define como "balance M negativo" al
# caso contrario.
joined = after_2000.fullOuterJoin(before_2000)


def how_balance(a, b):
    a1 = 0 if a == None else a
    b1 = 0 if b == None else b
    return a1 > b1


# ((cliente, mes), balance)
balances = joined.map(lambda x: ((x[0][0], x[0][1]), how_balance(x[1][0], x[1][1])))

# - Se desea saber cuantos contribuyentes tienen más de tres balances positivos
# y cuantos contribuyentes tienen más de tres balances negativos.
seqOp = lambda agg, line: (agg[0] + (1 if line else 0), agg[1] + (1 if not line else 0))
combOp = lambda x, y: (x[0] + y[0], x[1] + y[1])

count_balances = balances.map(lambda x: (x[0][0], x[1])).aggregateByKey(
    (0, 0), seqOp, combOp
)
count_balances_positive = count_balances.filter(lambda x: x[1][0] > 0).count()
count_balances_negative = count_balances.filter(lambda x: x[1][1] > 0).count()

print(
    " \
\nCantidad cont con +3 balances positivos {} \
\nCantidad cont con +3 balances negativos {}".format(
        count_balances_positive, count_balances_negative
    )
)
