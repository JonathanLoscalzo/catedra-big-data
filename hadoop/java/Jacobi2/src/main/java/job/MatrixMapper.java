package job;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MatrixMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();

		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line, "\"\t", false);
		String keyText = tokenizer.nextToken().toString();
		
		DoubleWritable aux = null;
		Double columnValue = 0.0;
		Double acum = 0.0;
		
		for (Long column = 0L; tokenizer.hasMoreTokens(); column++) {
			
			if (column == 0L) {
				columnValue = 1.0;
			} else {
				columnValue = conf.getDouble(new String("var" + column), 0.0);
			}
			
			acum += Double.parseDouble(tokenizer.nextToken().toString()) * columnValue;
			
		}
		
		context.write(new Text(keyText), new DoubleWritable(acum));
	}

}