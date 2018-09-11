package workers;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import soldProductsByOffice.SoldProductsByOfficeMapper;
import soldProductsByOffice.SoldProductsByOfficeReducer;
import writables.SucursalProductoWritable;

public class Worker extends Configured implements Tool {
	private static final String ITEMS_BY_OFFICE = "data/output/ItemsByOffice";
	private static final String PRODUCTS_BY_OFFICE = "data/output/ProductsByOffice";
	private static final String VENTAS_INPUT = "data/VentasInput";
	private static final String SOLD_PRODUCTS_BY_OFFICE = "data/output/SoldProductsByOffice";
	

	private Job setupJobSoldProductsByOffice(String[] args) throws IOException{		
		Configuration conf = getConf();   
	
		Job job = new Job(conf, SOLD_PRODUCTS_BY_OFFICE);
	    
	    job.setJarByClass(Worker.class);
	    
	    //configure Mapper
	    job.setMapperClass(SoldProductsByOfficeMapper.class);
	    job.setMapOutputKeyClass(SucursalProductoWritable.class);
	    job.setMapOutputValueClass(LongWritable.class);
	    
	    //configure Reducer
	    job.setReducerClass(SoldProductsByOfficeReducer.class);
	    // job.setCombinerClass(SoldProductsByOfficeReducer.class);
	    job.setOutputKeyClass(SucursalProductoWritable.class);
	    job.setOutputValueClass(LongWritable.class);
	    
	    job.setNumReduceTasks(3);
	    job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
	    
	    FileSystem fs = FileSystem.get(conf);
	    String inputDir = VENTAS_INPUT;//args[0];
	    String outputDir = SOLD_PRODUCTS_BY_OFFICE;//args[1];
	    if(fs.exists(new Path(outputDir))){	       
	       fs.delete(new Path(outputDir),true);
	    }	    

	    FileInputFormat.addInputPath(job, new Path(inputDir));
	    FileOutputFormat.setOutputPath(job, new Path(outputDir));
	    
	    return job;
	}
	
	@Override
	public int run(String[] args) throws Exception {
	    Job job;
	    boolean success;
	    
	    job = setupJobSoldProductsByOffice(args); 	    
	    success = job.waitForCompletion(true);
	    if (!success){
	    	System.out.println("Error job");
	    	return -1;
	    }
	    
	    job = WorkerProductsByOffice.setupJob(this.getConf(), SOLD_PRODUCTS_BY_OFFICE, PRODUCTS_BY_OFFICE);
	    success = job.waitForCompletion(true);
	    if (!success){
	    	System.out.println("Error job");
	    	return -1;
	    }
	    
	    job = WorkerItemsByOffice.setupJob(this.getConf(), SOLD_PRODUCTS_BY_OFFICE, ITEMS_BY_OFFICE);
	    success = job.waitForCompletion(true);
	    if (!success){
	    	System.out.println("Error job");
	    	return -1;
	    }
	    
	    job = WorkerMax.setupJob(this.getConf(), ITEMS_BY_OFFICE);
	    success = job.waitForCompletion(true);
	    if (!success){
	    	System.out.println("Error job");
	    	return -1;
	    }
	    
	    job = WorkerMax.setupJob(this.getConf(), PRODUCTS_BY_OFFICE);
	    success = job.waitForCompletion(true);
	    if (!success){
	    	System.out.println("Error job");
	    	return -1;
	    }
	    
	    return 0;
	}


}
		
