package wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//									   Input key	 Input val	  Output key	   Output val
public class WCMapper extends Mapper<LongWritable,   Text,        Text, 	   LongWritable>{
	
	private static LongWritable one = new LongWritable(1);
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{		
		Text word = new Text();
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line, "\"().,[]/-' ;", false);
	    while(tokenizer.hasMoreTokens()){
	    	word.set(tokenizer.nextToken());
	    	context.write(word, one); 	    	
	    }
	}
	
}
