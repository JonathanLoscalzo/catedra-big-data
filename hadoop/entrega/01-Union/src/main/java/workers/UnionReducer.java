package workers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UnionReducer extends Reducer<LongWritable, LongWritable, LongWritable, Text> {

	public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {

		context.write(key, new Text(""));
	}

}
