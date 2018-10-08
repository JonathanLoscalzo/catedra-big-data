package workers;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.collect.Lists;

public class DiffReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		List<Text> myList = Lists.newArrayList(values);
		Text first = myList.size() < 2 ? myList.get(0): null;
		
		if (myList.size() < 2 && first != null && first.toString().equals("A"))
			context.write(key, new Text(""));
		
	}

}
