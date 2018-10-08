package workers;

import org.apache.hadoop.util.ToolRunner;

public class Main {
	
	public static void main(String[] args) throws Exception{
		if (args.length < 3) {
			throw new Exception("debe tener al menos 3 parametros, (2 archivos entrada y 1 directorio salida)");
		}
		System.exit(ToolRunner.run(null, new Worker(), args));
	}
	
}


