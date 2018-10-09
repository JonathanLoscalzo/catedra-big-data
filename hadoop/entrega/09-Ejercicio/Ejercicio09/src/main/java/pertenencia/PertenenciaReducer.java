package pertenencia;

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
		if (key.get() == -1) {
			for (Text text : myList) {
				context.write(new LongWritable(Long.parseLong(text.toString())), new Text("SI"));
			}
		} else if (myList.size() == 2) {
			context.write(key, new Text("SI"));
		} else if (myList.get(0).toString().equals("Lista")) {
			context.write(key, new Text("NO"));
		}

	}

}
