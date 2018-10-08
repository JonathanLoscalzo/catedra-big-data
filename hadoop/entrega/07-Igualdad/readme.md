## Igualdad

### Fuente
El código fuente está en la carpeta "Igualdad".
Hay que exportarlo a un jar, con las librerías necesarias.
Es necesario agregar 2 archivos fuente, el primero es "lista" y el 2do el conjunto, y un directorio de salida.

Se utilizó la configuración para no tener que escribir N veces "No", con un "No" solo alcanza para saber que los conjuntos no son iguales.
-- ver la linea donde dice "context.getConfiguration();"

### Correrlo

Para correr el ejercicio, hay que tener en el hdfs una carpeta "Conjuntos" con todos los conjuntos. 

luego, podremos correr a partir del jar (tiene todo los jar's necesarios)

```bash
hadoop jar igualdad.jar E.txt A.txt Salida
hdfs dfs -copyToLocal Salida
```



