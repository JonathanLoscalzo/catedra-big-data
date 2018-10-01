package workers;

import java.util.logging.Level;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class Main {
	 /**
	355   * Default logging level for map/reduce tasks.
	356   */
	public static final Level DEFAULT_LOG_LEVEL = Level.INFO;
	
	public static void main(String[] args) throws Exception{
		System.exit(ToolRunner.run(new Configuration(true), new Worker(), args));
		//System.exit(ToolRunner.run(new Configuration(true), new Worker2(), args));
	}
	
}


