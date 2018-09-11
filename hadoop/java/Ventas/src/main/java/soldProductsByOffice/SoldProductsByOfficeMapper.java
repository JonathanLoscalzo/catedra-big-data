package soldProductsByOffice;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import writables.SucursalProductoWritable;

public class SoldProductsByOfficeMapper extends Mapper<LongWritable, Text, SucursalProductoWritable, LongWritable> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		ArrayList<String> words = new ArrayList<String>();
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line, "\"().,[]/-' ;\t", false);
		while (tokenizer.hasMoreTokens()) {
			words.add(tokenizer.nextToken().toString());
		}

		SucursalProductoWritable keyOutput = new SucursalProductoWritable(words.get(0), words.get(1));
		Long l = Long.parseLong(words.get(2));
		context.write(keyOutput, new LongWritable(l));
	}

}
