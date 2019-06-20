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


public class AirlineWithCodeShare extends Configured implements Tool {
	private static final String CODE_SHARE = "Y";
	/**
	 * process the routes data.
	 * @author matth
	 *
	 */
	public static class RoutesCodeShareMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		private int AIRLINE_ID_INDEX = 1;
		private int AIRLINE_CODE_SHARE_INDEX = 6;
		private IntWritable airlineId = new IntWritable();
		private Text codeshare = new Text();
		
		/**
		 * get the non-stop airline id and codeshare as output.
		 */
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] routeInfo = value.toString().split(",");
			if (StringUtils.isNumeric(routeInfo[AIRLINE_ID_INDEX]) && 
					routeInfo[AIRLINE_CODE_SHARE_INDEX].equals(CODE_SHARE)) {
				airlineId.set(Integer.parseInt(routeInfo[AIRLINE_ID_INDEX]));
				codeshare.set(CODE_SHARE);
				context.write(airlineId, codeshare);
			}
		}
	}
	
	/**
	 * process the airline data.
	 * @author matth
	 *
	 */
	public static class FinalAirlinesCodeShareMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		private int AIRLINE_ID_INDEX = 0;
		private int AIRLINE_NAME_INDEX = 1;
		private IntWritable airlineId = new IntWritable();
		private Text airlineName = new Text();
		
		/**
		 * get the airline id and name as output.
		 */
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] airlineInfo = value.toString().split(",");
			if (StringUtils.isNumeric(airlineInfo[AIRLINE_ID_INDEX])) {
				airlineId.set(Integer.parseInt(airlineInfo[AIRLINE_ID_INDEX]));
				airlineName.set(airlineInfo[AIRLINE_NAME_INDEX]);
				context.write(airlineId, airlineName);
			}
		}
	}
	
	/**
	 * join the airline id and write the non-stop airline name as output.
	 * @author matth
	 *
	 */
	public static class AirlineWithCodeShareReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		
		private Text airlineName = new Text();
		private IntWritable airlineId = new IntWritable();
		/**
		 * output the non-stop airline name.
		 * @param key airline id.
		 */
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			boolean haveCODE_SHARE = false;
			for (Text val : values) {
				if (val.toString().length() > 1) {
					airlineName.set(val.toString());
				} else if (val.toString().equals(CODE_SHARE)){
					haveCODE_SHARE = true;
				}
			}

			if (haveCODE_SHARE && airlineName.toString().length() > 1) {
				airlineId.set(Integer.parseInt(key.toString()));
				context.write(airlineId, airlineName);
			}	
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		 Job job = Job.getInstance(conf, "AirlineWithCodeShare");
		 job.setJarByClass(AirlineWithCodeShare.class);
		 MultipleInputs.addInputPath(job,new Path(args[0]), TextInputFormat.class, RoutesCodeShareMapper.class);
		 MultipleInputs.addInputPath(job,new Path(args[1]), TextInputFormat.class, FinalAirlinesCodeShareMapper.class);
		  
		 FileOutputFormat.setOutputPath(job, new Path(args[2]));
		 job.setReducerClass(AirlineWithCodeShareReducer.class);
		 job.setNumReduceTasks(1);
		 job.setOutputKeyClass(IntWritable.class);
		 job.setOutputValueClass(Text.class);
		 
		 return (job.waitForCompletion(true) ? 0 : 1);
	}
	
//	public static void main(String[] args) throws Exception {
//		int ecode = ToolRunner.run(new AirlineWithCodeShare(), args);
//		  System.exit(ecode);
//	}
}
