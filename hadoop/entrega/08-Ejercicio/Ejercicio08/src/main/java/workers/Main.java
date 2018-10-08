package workers;

import org.apache.hadoop.util.ToolRunner;

public class Main {

	public static void main(String[] args) throws Exception {
		if (args.length != 4) {
			throw new Exception("debe tener al menos 3 parametros, (3 archivos entrada y 1 directorio salida)");
		}
		
		if (args[0] == args[1] ||
				args[0] == args[2] ||
				args[1] == args[2]) {
			throw new Exception("Los conjuntos deben ser distintos, sino hadoop lo toma como 1 solo");
		}
		System.exit(ToolRunner.run(null, new Worker(), args));
	}

}
