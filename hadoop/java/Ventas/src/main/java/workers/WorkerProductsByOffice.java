package workers;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import productsByOffice.ProductsByOfficeMapper;
import productsByOffice.ProductsByOfficeReducer;

public class WorkerProductsByOffice {
	public String baseDir;

	public WorkerProductsByOffice() {
	}

	public static Job setupJob(Configuration conf, String input, String output) throws IOException {

		Job job = new Job(conf, "ProductsByOffice");

		job.setJarByClass(Worker.class);

		// configure Mapper
		job.setMapperClass(ProductsByOfficeMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		// configure Reducer
		job.setReducerClass(ProductsByOfficeReducer.class);
		// job.setCombinerClass(SoldProductsByOfficeReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		job.setNumReduceTasks(3);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileSystem fs = FileSystem.get(conf);
		String inputDir = input; // args[0];
		String outputDir = output; // args[1];
		if (fs.exists(new Path(outputDir))) {
			fs.delete(new Path(outputDir), true);
		}

		FileInputFormat.addInputPath(job, new Path(inputDir));
		FileOutputFormat.setOutputPath(job, new Path(outputDir));

		return job;
	}
}