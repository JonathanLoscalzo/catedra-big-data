## Enunciado - Ejercicio 9

Implemente un programa que lea tres conjuntos de datos A, B y C y una lista de elementos E y que realice
El cálculo del conjunto X del ejercicio 8, usando los conjuntos A, B y C.

```bash 
hadoop ejercicio8.jar A.txt B.txt C.txt conjunto_x
```

Que determine la cardinalidad de X.
```bash 
hadoop jar cardinal.jar conjunto_x cardinal_x
```

Que determine la pertenencia o no de los elementos de E en X.
```bash 
hadoop pertenencia.jar E.txt conjunto_x pertenencia_e_en_X
```

### Reusamos 
SI reusamos los anteriores jars, podemos correr en un bash el código en script.sh.
En un terminal: 
```
bash script.sh
```

Esto nos ejecuta los jar de ejercicio8, cardinal y pertenencia.
Luego, se copian los archivos de salida, en una carpeta del filesystem local. 
Se pueden observar 3 carpetas: cardinal_x, pertenencia_e_en_X, conjunto_x
Son las 3 salidas que necesitamos observar.

