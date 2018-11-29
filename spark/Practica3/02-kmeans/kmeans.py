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

lines = sc.textFile(arg1)
