from pyspark import SparkConf, SparkContext
from pyspark.sql import functions as f
from pyspark.sql import Row, SQLContext
from pyspark.sql.window import Window
import sys

conf = SparkConf().setMaster("local").setAppName("cantidad_viajes")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

textfile = "/tmp/data/Entrega2/trafico.txt" if (len(sys.argv) < 2) else sys.argv[1]

lines = sc.textFile(textfile).map(lambda a: a.split("\t")).map(lambda a: a[0:4])

# lines = lines.map(lambda a: Row(vehiculo=a[0], latitud = a[1], longitud = a[2], timestamp = a[3]))
df = sqlContext.createDataFrame(lines, ["vehiculo", "latitud", "longitud", "timestamp"])

"""
    Generamos un dataframe con todos los valores de la row anterior.
    Para encontrar "las rows anteriores" tenemos que tener las siguientes relaciones.
    una row anterior es:
    - un tiempo anterior
    - de un mismo auto

    De esta manera, podemos contar por cada elemento, cuantas cuadras fueron recorridas.
"""
w = Window.partitionBy(df.vehiculo).orderBy(df.timestamp.cast("int"))

# df = df.withColumn("vehiculo_prev", f.lag(df.vehiculo).over(w))
df = df.withColumn("lat_prev", f.lag(df.latitud).over(w))
df = df.withColumn("long_prev", f.lag(df.longitud).over(w))
df = df.withColumn("timestamp_prev", f.lag(df.timestamp).over(w))

# obtenemos por row, cuantas cuadras fueron recorridas por dicha latitud
# posibles valores, [0,1,None]
por_latitud = df.select(
    df.latitud, f.abs((df.longitud - df.long_prev)).alias("diff")
).where(f.col("diff") > 0)

por_longitud = df.select(
    df.longitud, f.abs((df.latitud - df.lat_prev)).alias("diff")
).where(f.col("diff") > 0)

""" 
opcion: 
por_latitud.groupBy("latitud")
    .sum("diff")
    .select("latitud", f.col("sum(diff)").alias("suma"))
    .sort("suma", ascending=False)
"""

# agrupamos por latitud, y sumarizamos las cuadras.
# nos quedamos con la latitud que tiene mayor sumatoria
max_lat = (
    por_latitud.groupBy("latitud")
    .agg(f.sum("diff").alias("suma"))
    .sort("suma", ascending=False)
    .take(1)
)

max_long = (
    por_longitud.groupBy("longitud")
    .agg(f.sum("diff").alias("suma"))
    .sort("suma", ascending=False)
    .take(1)
)

print(
    "\n Calle en longitud {} recorrida {} \n".format(
        max_long[0].longitud, max_long[0].suma
    )
)

print(
    "\n Calle en latitud {} recorrida {} \n".format(max_lat[0].latitud, max_lat[0].suma)
)
