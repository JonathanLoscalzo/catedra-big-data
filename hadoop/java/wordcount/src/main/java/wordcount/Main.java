package wordcount;

import org.apache.hadoop.util.ToolRunner;

public class Main {
	
	public static void main(String[] args) throws Exception{
		System.exit(ToolRunner.run(null, new Worker(), args));
	}
	
}


