package workers;

import org.apache.hadoop.util.ToolRunner;

public class Main {

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			throw new Exception("debe tener al menos 2 parametros, (1 archivos entrada y 1 directorio salida)");
		}
		System.exit(ToolRunner.run(null, new Worker(), args));
	}

}
