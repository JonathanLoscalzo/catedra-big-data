package workers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

public class Worker extends Configured implements Tool {
	private static final String PERTENENCIA_E_EN_X = "Salida/pertenencia_e_en_X";
	private static final String CARDINAL_X = "Salida/cardinal_x";
	private static final String CONJUNTO_X_SALIDA = "Conjuntos/conjunto_x";
	private static final String CONJUNTO_X = "conjunto_x";
	String baseDir;

	@Override
	public int run(String[] args) throws Exception {
		Job job;
		boolean success;
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);

		String outputDir = "Salida";
		if (fs.exists(new Path(outputDir))) {
			fs.delete(new Path(outputDir), true);
		}
				
		job = ejercicio8.JobFactory.setupJob(conf, new String[] { args[0], args[1], args[2], CONJUNTO_X_SALIDA });

		success = job.waitForCompletion(true);
		if (!success) {
			System.err.println("Error job");
			return -1;
		}

		job = cardinal.JobFactory.setupJob(conf, new String[] { CONJUNTO_X, CARDINAL_X });
		success = job.waitForCompletion(true);
		if (!success) {
			System.err.println("Error job");
			return -1;
		}
		
		job = pertenencia.JobFactory.setupJob(conf, new String[] { args[3], CONJUNTO_X, PERTENENCIA_E_EN_X });
		success = job.waitForCompletion(true);
		if (!success) {
			System.err.println("Error job");
			return -1;
		}

		return 0;
	}

}
