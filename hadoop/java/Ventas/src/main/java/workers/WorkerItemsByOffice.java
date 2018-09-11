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

import itemsByOffice.ItemsByOfficeMapper;
import itemsByOffice.ItemsByOfficeReducer;

public class WorkerItemsByOffice {
	public String baseDir;

	public WorkerItemsByOffice() {
	}

	public static Job setupJob(Configuration conf, String input, String output) throws IOException {

		Job job = new Job(conf, "ItemsByOffice");

		job.setJarByClass(Worker.class);

		// configure Mapper
		job.setMapperClass(ItemsByOfficeMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		// configure Reducer
		job.setReducerClass(ItemsByOfficeReducer.class);
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