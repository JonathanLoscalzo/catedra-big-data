from pyspark import SparkConf, SparkContext
import sys

arg1 = sys.argv[1]

conf = SparkConf().setMaster("local").setAppName("Sales")
sc = SparkContext(conf=conf)

text_file = sc.textFile(arg1)

lines = text_file.map(lambda line: line.split("\t"))

# acumulo por producto por ende no se repiten productos por sucursal
mapSucProd = lines.map(lambda line: ((line[0], line[1]), line[2]))
acummulator = mapSucProd.reduceByKey(lambda a, b: a + b)

# modifico las claves del acumulador para que solo la sucursal sea clave
acummulator = acummulator.map(lambda x: (x[0][0], (x[0][1], int(x[1]))))

# Producto más vendido por sucursal
maxSucProd = acummulator.reduceByKey(lambda a, b: max(a, b, key=lambda x: x[1]))
maxSucProd.saveAsTextFile("ProductoMasVendidoPorSucursal")

# Sucursal que más productos distintos vendió
maxProdDist = acummulator.map(lambda a: (a[0], 1)).reduceByKey(lambda a, b: a + b)

# no se como se escribe en el hdfs
print(
    "Sucursal que más productos distintos vendió {}".format(
        maxProdDist.reduce(lambda a, b: max(a, b, key=lambda x: x[1]))
    )
)

# Sucursal que más ítems vendió
maxProdxSuc = acummulator.map(lambda a: (a[0], int(a[1][1]))).reduceByKey(
    lambda a, b: a + b
)

print(
    "SucursalMasItemsVendio {}".format(
        maxProdxSuc.reduce(lambda a, b: max(a, b, key=lambda x: x[1]))
    )
)

# prácticamente las cuentas son todas iguales

