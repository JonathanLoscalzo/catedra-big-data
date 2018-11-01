## EJERCICIO 1
 Compile y ejecute el proyecto WordCount
dado por la cátedra. Utilice para ello el
dataset Libros

## EJERCICIO 2
 Rehaga el ejercicio 1 de la clase 3 en Spark
 El dataset Ventas.rar tiene información sobre las ventas
realizadas durante el último mes por distintas sucursales de
una cadena de ferreterías. El formato de los datos del
dataset es:
<id_sucursal, id_producto, cantidad_vendida>
 Calcule:
◦ Producto más vendido por sucursal
◦ Sucursal que más productos distintos vendió
◦ Sucursal que más ítems vendió

## EJERCICIO 3
 Rehaga el ejercicio 2 de la clase 5 en Spark
 Los archivos estacionSur.txt y estacionNorte.txt tienen información
sobre datos climáticos (temperatura, humedad y precipitación).
◦ estacionNorte.txt almacena la información en grados centígrados,
porcentaje de humedad, y milímetros de lluvia.
◦ estacionSur.txt almacena la información en grados fahrenheit, porcentaje de
humedad, y centímetros de lluvia.
 Arme una aplicación en Spark que obtenga el promedio de
temperatura, de humedad y precipitación total entre todas las
estaciones. Además deberá informar la estación más fría, la más
calurosa, la más húmeda, la menos húmeda, la más lluviosa y la
menos lluviosa.

## Ejercicio 4
 Rehaga el ejercicio 3 de la clase 5 en Spark
 El departamento fiscal de la ciudad de La Plata almacena para sus
contribuyentes el monto de diferentes impuestos discriminados por mes y año.
 Resulta de interés comparar para un mismo contribuyente y en un
determinado mes dos períodos históricos diferentes ("anterior al 2000" y
"después del 2000").
 Se define como "balance M positivo" si para un contribuyente el monto total de
impuestos pagados en el mes M es mayor en el período "después del 2000"
que en el período "anterior al 2000". Se define como "balance M negativo" al
caso contrario.
 Se desea saber cuantos contribuyentes tienen más de tres balances positivos
y cuantos contribuyentes tienen más de tres balances negativos.