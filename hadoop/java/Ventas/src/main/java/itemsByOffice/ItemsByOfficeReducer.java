package itemsByOffice;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ItemsByOfficeReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	public void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {

		Long acum = 0L;
		
		for (LongWritable val : values) {
			acum += val.get();
		}

		context.write(key, new LongWritable(acum));
	}

}
