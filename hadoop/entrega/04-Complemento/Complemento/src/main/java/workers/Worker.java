package workers;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

public class Worker extends Configured implements Tool {
	String baseDir;	
	
	private Job setupJob(Configuration conf, String[] args) throws IOException{		
		Job job = new Job(conf, "Intersection job");
	    
	    job.setJarByClass(Worker.class);
	    
	    //configure Reducer
	    job.setReducerClass(DiffReducer.class);
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(Text.class);
	    
	    job.setMapOutputKeyClass(LongWritable.class);
	    job.setMapOutputValueClass(Text.class);
	   
	    // job.setInputFormatClass(TextInputFormat.class);
	    // job.setOutputFormatClass(TextOutputFormat.class);
	    
	    MultipleInputs.addInputPath(job, new Path("Conjuntos/"+args[0]), TextInputFormat.class, UniversoMapper.class);
	    MultipleInputs.addInputPath(job, new Path("Conjuntos/"+args[1]), TextInputFormat.class, ConjuntoMapper.class);
	    
	    job.setNumReduceTasks(3);
	    
	    FileSystem fs = FileSystem.get(conf);
	    
	    String outputDir = args[2];
	    if(fs.exists(new Path(outputDir))){	       
	       fs.delete(new Path(outputDir),true);
	    }	    
	    
	    //FileInputFormat.addInputPath(job, new Path("Conjuntos"));
	    FileOutputFormat.setOutputPath(job, new Path(outputDir));
	    
	    return job;
	}
	
	@Override
	public int run(String[] args) throws Exception {
	    Job job;
	    boolean success;
	    
	    job = setupJob(getConf(), args); 	    
	    success = job.waitForCompletion(true);
	    if (!success){
	    	System.err.println("Error job");
	    	return -1;
	    }
	    
	    return 0;
	}


}
		
