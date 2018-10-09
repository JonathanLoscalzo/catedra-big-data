package ejercicio8;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.collect.Lists;

public class UnicoReducer extends Reducer<LongWritable, LongWritable, LongWritable, Text> {

	public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {

		Long acum = 0L;
		for (LongWritable value : values) {
			acum += value.get();
		}
		
		if (acum == 1) {			
			context.write(key, new Text(""));
		}
		
	}

}
