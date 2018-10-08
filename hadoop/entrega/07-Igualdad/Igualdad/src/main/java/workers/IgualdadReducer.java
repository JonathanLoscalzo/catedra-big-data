package workers;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.collect.Lists;

public class IgualdadReducer extends Reducer<LongWritable, Text, Text, Text> {

	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		List<Text> myList = Lists.newArrayList(values);
		
		if (conf.getBoolean("ESCRIBIR", true) && myList.size() != 2) {			
			context.write(new Text("NO"), new Text(""));
			conf.setBoolean("ESCRIBIR", false); // de esta manera escribimos uno solo
		}
		
	}

}
