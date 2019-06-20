import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ActiveAirlineInCountry {
	private static final int AIRLINE_NAME_INDEX = 1;
	private static final int IATA_INDEX = 3;
	private static final int COUNTRY_INDEX = 6;
	private static final int ACTIVE_INDEX = 7;
	private static final int IATA_LENGTH = 2;
	private static final int ACTIVE_CODE_LENGTH = 1;
	private static final String COUNTRY = "United States";
	private static final String ACTIVE_CODE = "Y";
	private static final int AIRLINE_ID_INDEX = 0;
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Active Airline In Country");
		job.setJarByClass(ActiveAirlineInCountry.class);
		job.setMapperClass(ActiveAirlineInCountryMapper.class);
		job.setReducerClass(ActiveAirlineInCountryReducer.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static class ActiveAirlineInCountryMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		
		
		private Text airlineNameCountryIataActive = new Text();
		private IntWritable airlineId = new IntWritable();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] airlineInfo = value.toString().split(",");
			if (airlineInfo[ACTIVE_INDEX].equalsIgnoreCase(ACTIVE_CODE) &&
					airlineInfo[IATA_INDEX].length() == IATA_LENGTH &&
					StringUtils.isAlpha(airlineInfo[IATA_INDEX]) &&
					airlineInfo[COUNTRY_INDEX].equalsIgnoreCase(COUNTRY)) {
				
				airlineId.set(Integer.parseInt(airlineInfo[AIRLINE_ID_INDEX]));
				airlineNameCountryIataActive.set(airlineInfo[AIRLINE_NAME_INDEX] + 
						"," + airlineInfo[COUNTRY_INDEX] + "," + airlineInfo[IATA_INDEX] +
						"," + airlineInfo[ACTIVE_INDEX]);
				context.write(airlineId, airlineNameCountryIataActive);
			}
			
		}
	}
	
	
	public static class ActiveAirlineInCountryReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		private IntWritable airlineId = new IntWritable();
		/**
		 * the output is airline name, IATA code, Country, Active Code.
		 * @param airlineId 
		 * @param values airline name, IATA code, Country, Active Code
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		public void reduce(IntWritable airlineId, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text info : values) {
				context.write(airlineId, info);
			}
		}
	}
}
