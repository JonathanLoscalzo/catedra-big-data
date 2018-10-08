## Ejercicio 8

### Fuente
El código fuente está en la carpeta "Ejercicio8".
Hay que exportarlo a un jar, con las librerías necesarias.
Es necesario agregar 3 archivos fuente DISTINTOS y un directorio de salida.

### Correrlo

Para correr el ejercicio, hay que tener en el hdfs una carpeta "Conjuntos" con todos los conjuntos. 

luego, podremos correr a partir del jar (tiene todo los jar's necesarios)

```bash
hadoop jar ejercicio8.jar A.txt B.txt C.txt Salida
hdfs dfs -copyToLocal Salida
```



