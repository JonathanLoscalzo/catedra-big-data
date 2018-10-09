package pertenencia;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.collect.Lists;

public class PertenenciaCombiner extends Reducer<LongWritable, Text, LongWritable, Text> {
	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		List<Text> myList = Lists.newArrayList(values);
		
		if (myList.size() == 2) {
			context.write(new LongWritable(-1), new Text(key.toString()));
		} else {
			for (Text text : myList) {
				context.write(key, text);
			}
		}
		
	}
}
