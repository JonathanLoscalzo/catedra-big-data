## Pertenencia

### Fuente
El código fuente está en la carpeta "Pertenencia".
Hay que exportarlo a un jar, con las librerías necesarias.
Es necesario agregar 2 archivos fuente, el primero es "lista" y el 2do el conjunto, y un directorio de salida.

### Correrlo

Para correr el ejercicio, hay que tener en el hdfs una carpeta "Conjuntos" con todos los conjuntos. 

luego, podremos correr a partir del jar (tiene todo los jar's necesarios)

```bash
hadoop jar pertenencia.jar E.txt A.txt Salida
hdfs dfs -copyToLocal Salida
```



