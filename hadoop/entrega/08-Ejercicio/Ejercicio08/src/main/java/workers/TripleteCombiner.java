package workers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class TripleteCombiner extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
	public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {

		Long acum = 0L;
		for (LongWritable value : values) {
			acum += value.get();
		}
		
		if (acum != 3) {			
			context.write(key, new LongWritable(acum));
		}
		
	}
}
