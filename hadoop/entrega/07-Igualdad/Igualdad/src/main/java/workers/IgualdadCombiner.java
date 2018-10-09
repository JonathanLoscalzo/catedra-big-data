package workers;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.collect.Lists;

public class IgualdadCombiner extends Reducer<LongWritable, Text, LongWritable, Text> {

	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		List<Text> myList = Lists.newArrayList(values);

		// SI es igual a 2, no lo escribo porque es un no.
		// el condicional podrìa tener conf.getBoolean("ESCRIBIR", true)
		if (myList.size() != 2) {
			
			for (Text text : myList) {
				context.write(key, text);
			}
			
		}
	}
}
