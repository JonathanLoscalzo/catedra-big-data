package itemsByOffice;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ItemsByOfficeMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		ArrayList<String> words = new ArrayList<String>();
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line, "\"().,[]/-' ;\t", false);
		while (tokenizer.hasMoreTokens()) {
			words.add(tokenizer.nextToken().toString());
		}

		context.write(new Text(words.get(0)), new LongWritable(Long.parseLong(words.get(2))));
	}

}
