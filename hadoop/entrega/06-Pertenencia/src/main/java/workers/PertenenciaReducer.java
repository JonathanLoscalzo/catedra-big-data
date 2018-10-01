package workers;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.collect.Lists;

public class PertenenciaReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		List<Text> myList = Lists.newArrayList(values);
		context.write(key, new Text(myList.size() == 2 ? "SI": "NO"));
		
	}

}
