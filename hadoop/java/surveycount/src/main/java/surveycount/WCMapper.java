package surveycount;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import me.xdrop.fuzzywuzzy.FuzzySearch;

public class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	private static LongWritable one = new LongWritable(1);
	private static ArrayList<String> optionKeys = new ArrayList<String>(
			Arrays.asList("Muy satisfecho", "Algo satisfecho", "Poco satisfecho", "Muy disconforme"));

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		for (String e : optionKeys) {
			if (FuzzySearch.ratio(e, value.toString()) > 90) {
				 context.write(new Text(e), one);
				 return;
			}
		}
		
		context.write(new Text("NOT MATCHING"), one);
	}

}
