package wordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class WCReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		
		int times = 0;
		for (@SuppressWarnings("unused") Object val : values) {
			times++;
		}
		
		context.write(key, new LongWritable(times));
	}
	
}
