import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class FindNonStopAirline {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Find airport");
		job.setJarByClass(FindAirportInCountry.class);
		MultipleInputs.addInputPath(job, new Path(args[0]), 
				TextInputFormat.class, RoutesMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), 
				TextInputFormat.class, FinalAirlinesMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.setReducerClass(FindNonStopAirlineReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	/**
	 * process the routes data.
	 * @author matth
	 *
	 */
	public static class RoutesMapper extends Mapper<LongWritable, Text, Text, Text> {
		private final int STOPS_INDEX = 7;
		private final int AIRLINE_ID_INDEX = 1;
		private Text airlineId = new Text();
		private Text dummyVal = new Text("0");
		
		/**
		 * get the non-stop airline id.
		 */
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] routeInfo = value.toString().split(",");
			if (routeInfo[STOPS_INDEX].equals("0") && StringUtils.isNumeric(routeInfo[AIRLINE_ID_INDEX])) {
				airlineId.set(routeInfo[AIRLINE_ID_INDEX]);
				context.write(airlineId, dummyVal);
			}
		}
	}
	
	/**
	 * process the airline data.
	 * @author matth
	 *
	 */
	public static class FinalAirlinesMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text airlineName = new Text();
		private Text airlineId = new Text();
		private final int AIRLINE_ID_INDEX = 0;
		private final int AIRLINE_NAME_INDEX = 1;
		
		/**
		 * get the airline id and name.
		 */
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] airlineInfo = value.toString().split(",");
			if (StringUtils.isNumeric(airlineInfo[AIRLINE_ID_INDEX])) {
				airlineName.set(airlineInfo[AIRLINE_NAME_INDEX]);
				airlineId.set(airlineInfo[AIRLINE_ID_INDEX]);
				context.write(airlineId, airlineName);
			}
		}
	}
	
	/**
	 * join the airline id and write the non-stop airline name as output.
	 * @author matth
	 *
	 */
	public static class FindNonStopAirlineReducer extends Reducer<Text, Text, Text, Text> {
		private Text dummyKey = new Text();
		/**
		 * output the non-stop airline name.
		 */
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text val : values) {
				if (!val.equals("0")) {
					context.write(dummyKey, val);
				}
			}
		}
	}
}
