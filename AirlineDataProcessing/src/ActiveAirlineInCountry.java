import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
	
//	public static void main(String[] args) throws Exception {
//		Configuration conf = new Configuration();
//		Job job = Job.getInstance(conf, "Active Airline In Country");
//		job.setJarByClass(ActiveAirlineInCountry.class);
//		job.setMapperClass(ActiveAirlineInCountryMapper.class);
//		job.setReducerClass(ActiveAirlineInCountryReducer.class);
//		
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(Text.class);
//		
//		FileInputFormat.addInputPath(job, new Path(args[0]));
//		FileOutputFormat.setOutputPath(job, new Path(args[1]));
//		
//		System.exit(job.waitForCompletion(true) ? 0 : 1);
//	}
	
	public static class ActiveAirlineInCountryMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text activeCode = new Text();
		private Text airlineNameTataCountry = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] airlineInfo = value.toString().split(",");
			if (!StringUtils.isEmpty(airlineInfo[ACTIVE_INDEX]) &&
					airlineInfo[ACTIVE_INDEX].length() == ACTIVE_CODE_LENGTH &&
					!StringUtils.isEmpty(airlineInfo[AIRLINE_NAME_INDEX]) &&
					!StringUtils.isEmpty(airlineInfo[IATA_INDEX]) && 
					airlineInfo[IATA_INDEX].length() == IATA_LENGTH && 
					!StringUtils.isEmpty(airlineInfo[COUNTRY_INDEX]) &&
					airlineInfo[COUNTRY_INDEX].equalsIgnoreCase(COUNTRY) && 
					airlineInfo[ACTIVE_INDEX].equalsIgnoreCase(ACTIVE_CODE)) {
				
				activeCode.set(airlineInfo[ACTIVE_INDEX]);
				airlineNameTataCountry.set(airlineInfo[AIRLINE_NAME_INDEX] + ", " + airlineInfo[IATA_INDEX] + ", " + 
				airlineInfo[COUNTRY_INDEX]);
				context.write(activeCode, airlineNameTataCountry);
			}
			
		}
	}
	
	
	public static class ActiveAirlineInCountryReducer extends Reducer<Text, Text, Text, Text> {
		
		/**
		 * the output is airline name, IATA code, Country, Active Code
		 * @param activeCode active code
		 * @param values airline names
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		public void reduce(Text activeCode, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text info : values) {
				context.write(info, activeCode);
			}
		}
	}
}
