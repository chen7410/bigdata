import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class FindNonStopAirline extends Configured implements Tool {
	
	/**
	 * process the routes data.
	 * @author matth
	 *
	 */
	public static class RoutesMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		private final int STOPS_INDEX = 7;
		private final int AIRLINE_ID_INDEX = 1;
		private IntWritable airlineId = new IntWritable();
		private Text dummyVal = new Text("0");
		
		/**
		 * get the non-stop airline id.
		 */
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] routeInfo = value.toString().split(",");
			if (routeInfo[STOPS_INDEX].equals("0") && StringUtils.isNumeric(routeInfo[AIRLINE_ID_INDEX])) {
				airlineId.set(Integer.parseInt(routeInfo[AIRLINE_ID_INDEX]));
				context.write(airlineId, dummyVal);
			}
		}
	}
	
	/**
	 * process the airline data.
	 * @author matth
	 *
	 */
	public static class FinalAirlinesMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		private Text airlineName = new Text();
		private IntWritable airlineId = new IntWritable();
		private final int AIRLINE_ID_INDEX = 0;
		private final int AIRLINE_NAME_INDEX = 1;
		
		/**
		 * get the airline id and name.
		 * 
		 */
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] airlineInfo = value.toString().split(",");
			if (StringUtils.isNumeric(airlineInfo[AIRLINE_ID_INDEX])) {
				airlineName.set(airlineInfo[AIRLINE_NAME_INDEX]);
				airlineId.set(Integer.parseInt(airlineInfo[AIRLINE_ID_INDEX]));
				context.write(airlineId, airlineName);
			}
		}
	}
	
	/**
	 * write non-stop airline id and the airline name as output.
	 * @author matth
	 *
	 */
	public static class FindNonStopAirlineReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		private String STOPS = "0";
		private Text airlineName = new Text();
		private IntWritable airlineId = new IntWritable();
		/**
		 * output the non-stop airline name.
		 * @param key airline id.
		 */
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			boolean haveSTOPS  = false;
			for (Text val : values) {
				if (val.toString().length() > 1) {
					airlineName.set(val);
				}
				if (val.toString().equals(STOPS)) {
					haveSTOPS = true;
				}
			}
			//the values collection must has 0 and airline name
			if (haveSTOPS && airlineName.toString().length() > 1) {
				airlineId.set(Integer.parseInt(key.toString()));
				context.write(airlineId, airlineName);
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		 Job job = Job.getInstance(conf, "FindNonStopAirline");
		 job.setJarByClass(FindNonStopAirline.class);
		 MultipleInputs.addInputPath(job,new Path(args[0]), TextInputFormat.class, RoutesMapper.class);
		 MultipleInputs.addInputPath(job,new Path(args[1]), TextInputFormat.class, FinalAirlinesMapper.class);
		  
		 FileOutputFormat.setOutputPath(job, new Path(args[2]));
		 job.setReducerClass(FindNonStopAirlineReducer.class);
		 job.setNumReduceTasks(1);
		 job.setOutputKeyClass(IntWritable.class);
		 job.setOutputValueClass(Text.class);
		 
		 return (job.waitForCompletion(true) ? 0 : 1);
	}
	
//	public static void main(String[] args) throws Exception {
//		int ecode = ToolRunner.run(new FindNonStopAirline(), args);
//		  System.exit(ecode);
//	}
}
