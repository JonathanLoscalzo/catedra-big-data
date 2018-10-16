hadoop jar ejercicio9.jar A.txt B.txt C.txt E.txt
rm -rf Salida
hdfs dfs -copyToLocal Salida $PWD/Salida
hdfs dfs -copyToLocal Conjuntos/conjunto_x $PWD/Salida