package pertenencia;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ListaMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		ArrayList<String> words = new ArrayList<String>();
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line, "\"().,[]/-' ;\t", false);
	    
		while(tokenizer.hasMoreTokens()){
	    	words.add(tokenizer.nextToken().toString());    	
	    }
	    
	    context.write(new LongWritable(Long.parseLong(words.get(0))), new Text("Lista")); 
	}

}
