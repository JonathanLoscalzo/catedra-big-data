#!/bin/bash

hadoop jar $PWD/ejercicio8.jar A.txt B.txt C.txt Conjuntos/conjunto_x
hadoop jar $PWD/cardinal.jar conjunto_x Salida/cardinal_x
hadoop jar $PWD/pertenencia.jar E.txt conjunto_x Salida/pertenencia_e_en_X

rm -rf Salida
hdfs dfs -copyToLocal Salida $PWD/Salida
hdfs dfs -copyToLocal Conjuntos/conjunto_x $PWD/Salida