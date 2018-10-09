package ejercicio8;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import workers.Worker;

public class JobFactory {
	public static Job setupJob(Configuration conf, String[] args) throws IOException{		
		Job job = new Job(conf, "Intersection job");
	    
	    job.setJarByClass(Worker.class);
	    
	    //configure Reducer
	    job.setReducerClass(UnicoReducer.class);
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(Text.class);
	    
	    job.setMapOutputKeyClass(LongWritable.class);
	    job.setMapOutputValueClass(LongWritable.class);
	    job.setCombinerClass(TripleteCombiner.class);
	   
	    // job.setInputFormatClass(TextInputFormat.class);
	    // job.setOutputFormatClass(TextOutputFormat.class);
	    
	    MultipleInputs.addInputPath(job, new Path("Conjuntos/"+args[0]), TextInputFormat.class, ConjuntoMapper.class);
	    MultipleInputs.addInputPath(job, new Path("Conjuntos/"+args[1]), TextInputFormat.class, ConjuntoMapper.class);
	    MultipleInputs.addInputPath(job, new Path("Conjuntos/"+args[2]), TextInputFormat.class, ConjuntoMapper.class);
	    
	    job.setNumReduceTasks(3);
	    
	    FileSystem fs = FileSystem.get(conf);
	    
	    String outputDir = args[3];
	    if(fs.exists(new Path(outputDir))){	       
	       fs.delete(new Path(outputDir),true);
	    }	    
	    
	    //FileInputFormat.addInputPath(job, new Path("Conjuntos"));
	    FileOutputFormat.setOutputPath(job, new Path(outputDir));
	    
	    return job;
	}
}
