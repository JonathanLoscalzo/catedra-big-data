## Ej2
### Implemente una aplicación MapReduce

Implemente una aplicación MapReduce para contabilizar cuantos clientes están "Muy satisfecho", "Algo satisfecho", "Poco satisfecho" y "Muy disconforme".

### Correr:

Necesita las entradas "Encuestas" y "EncuestasOut"

### Nota: 
Se agrega [esta la libreria para string-matching](https://github.com/xdrop/fuzzywuzzy).
Calculamos que tan iguales son las palabras, para hacer un mejor filtrado.  

```
if (FuzzySearch.ratio(e, value.toString()) > 90) {
				 context.write(new Text(e), one);
				 return;
			}
```

Las que no funcionan, las contamos en errores.  

```
context.write(new Text("NOT MATCHING"), one);
```