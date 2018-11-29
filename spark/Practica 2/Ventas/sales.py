from pyspark import SparkConf, SparkContext
from pyspark.sql import Row, SQLContext

import sys

arg1 = sys.argv[1]

conf = SparkConf().setMaster("local").setAppName("Sales")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

text_file = sc.textFile(arg1)

df = text_file.map(lambda line: line.split("\t")).map(
    lambda x: Row(sucursal=x[0], producto=x[1], cantidad=int(x[2]))
)

df = sqlContext.createDataFrame(df)
df.registerTempTable("venta")

# Producto más vendido por sucursal
sum_suc_product = df.select("sucursal", "producto", "cantidad").groupBy("sucursal", "producto").sum("cantidad")
max_sum_suc = sum_suc_product.groupBy("sucursal").max("sum(cantidad)")
max_sum_suc.join(sum_suc_product, [sum_suc_product["sum(cantidad)"] == max_sum_suc["max(sum(cantidad))"] , "sucursal"] ).show()
#sum_suc_product.filter(sum_suc_product["sucursal"] == 44)


# Sucursal que más productos distintos vendió
counter_product = df.select("sucursal", "producto").distinct().groupBy("sucursal").count()
max_product = counter_product.groupBy().max("count")
counter_product.join(max_product, counter_product["count"] == max_product["max(count)"]).show()

# counters = sqlContext.sql("SELECT COUNT(DISTINCT producto) AS counter, sucursal FROM venta GROUP BY sucursal")
# counters.registerTempTable("count_producto")
#__________________

# Sucursal que más ítems vendió
sum_product = df.select("sucursal", "producto", "cantidad").groupBy("sucursal").sum("cantidad")
max_sum_product = sum_product.groupBy().max("sum(cantidad)")
sum_product.join(max_sum_product, sum_product["sum(cantidad)"] == max_sum_product["max(sum(cantidad))"]).show()
# prácticamente las cuentas son todas iguales

