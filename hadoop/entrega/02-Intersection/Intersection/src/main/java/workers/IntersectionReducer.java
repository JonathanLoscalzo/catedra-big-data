package workers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.collect.Lists;

public class IntersectionReducer extends Reducer<LongWritable, LongWritable, LongWritable, Text> {

	public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {

		if (Lists.newArrayList(values).size() == 2)
			context.write(key, new Text(""));
	}

}
