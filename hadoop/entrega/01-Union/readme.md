### Fuente
El código fuente está en la carpeta "Union".
Hay que exportarlo a un jar, con las librerías necesarias.
Es necesario agregar 2 archivos fuente, y un directorio de salida.

### Correrlo

Para correr el ejercicio union, hay que tener en el hdfs una carpeta "Conjuntos" con todos los conjuntos. 

luego, podremos correr a partir del jar (tiene todo los jar's necesarios)

```bash
hadoop jar union.jar A.txt B.txt Salida
hdfs dfs -copyToLocal Salida
```



