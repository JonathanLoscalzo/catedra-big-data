package soldProductsByOffice;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import writables.SucursalProductoWritable;

public class SoldProductsByOfficeReducer extends Reducer<SucursalProductoWritable, LongWritable, SucursalProductoWritable, LongWritable> {

	public void reduce(SucursalProductoWritable key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {

		Long acum = 0L;
		
		for (LongWritable val : values) {
			acum += val.get();
		}

		context.write(key, new LongWritable(acum));
	}

}
