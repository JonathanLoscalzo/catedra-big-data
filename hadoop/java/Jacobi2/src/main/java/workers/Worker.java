package workers;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

import job.MatrixWorker;

public class Worker extends Configured implements Tool {
	private String JACOBI_INPUT = "Jacobi/input";
	private String MATRIX_OUTPUT = "Jacobi/matrix/output";
	private String ERROR_OUTPUT = "Jacobi/error/output";
	private String JACOBI_OUTPUT = "Jacobi/output";

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();

		if (runJobs(conf) < 0)
			return -1;
		
		int times = 0;

		while (obtenerDiferencia(conf) >= 0.001 && times < 10) {
			setearNuevosValoresIntermedios(conf);
			times++;

			if (runJobs(conf) < 0)
				return -1;
		}
		
		System.err.println("TIMES: "+times);
		return 0;
	}

	private int runJobs(Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {

		Job job;
		boolean success;

		System.err.println("MatrixWorker");
		job = MatrixWorker.setupJob(conf, JACOBI_INPUT, JACOBI_OUTPUT);

		success = job.waitForCompletion(true);
		if (!success) {
			System.out.println("Error job");
			return -1;
		}

		return 0;
	}

	private void setearNuevosValoresIntermedios(Configuration conf) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path(JACOBI_OUTPUT), false);
		
		while (files.hasNext()) {
			LocatedFileStatus file = files.next();
			System.err.println(file.getPath());
			FSDataInputStream fin = fs.open(file.getPath());
			String line = fin.readLine();
			while (line != null) {
				String[] tokenizer = line.split("\t");
				conf.setDouble(tokenizer[0], Double.parseDouble(tokenizer[1]));
				System.err.println(tokenizer[0]+"-"+tokenizer[1]);
				line = fin.readLine();
			}
			
			fin.close();
		}
	}

	private Double obtenerDiferencia(Configuration conf) throws IOException, URISyntaxException {
		
		FileSystem fs = FileSystem.get(conf);
		RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path(JACOBI_OUTPUT), false);
		Double diff = 0.0;
		while (files.hasNext()) {
			LocatedFileStatus file = files.next();
			FSDataInputStream fin = fs.open(file.getPath());
			String line = fin.readLine();
			while (line != null) {
				String[] tokenizer = line.split("\t");
				Double nuevo = Double.parseDouble(tokenizer[1]);
				Double anterior = conf.getDouble(tokenizer[0], 0);
				diff += Math.pow(nuevo - anterior, 2);
				line = fin.readLine();
			}
			
			fin.close();
		}
		
		return diff;
	}

}
