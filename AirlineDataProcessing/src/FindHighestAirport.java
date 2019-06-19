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

public class FindHighestAirport {
	private static final int ALTITUDE_INDEX = 8;
	private static final int AIRPORT_NAME_INDEX = 2;
	private static final int COUNTRY_INDEX = 3;
	
//	public static void main(String[] args) throws Exception {
//		Configuration conf = new Configuration();
//		Job job = Job.getInstance(conf, "Find highest airport");
//		job.setJarByClass(FindHighestAirport.class);
//		job.setMapperClass(FindHighestAirportMapper.class);
//		job.setReducerClass(FindHighestAirportReducer.class);
//		
//		job.setOutputKeyClass(IntWritable.class);
//		job.setOutputValueClass(Text.class);
//		
//		FileInputFormat.addInputPath(job, new Path(args[0]));
//		FileOutputFormat.setOutputPath(job, new Path(args[1]));
//		
//		System.exit(job.waitForCompletion(true) ? 0 : 1);
//	}
	
	public static class FindHighestAirportMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		private Text airportNameCountry = new Text();
		private IntWritable altitude = new IntWritable();
		/**
		 * get the altitude and airport name
		 */
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] airportInfo = value.toString().split(",");
			if (StringUtils.isNumeric(airportInfo[ALTITUDE_INDEX])) {
				altitude.set(Integer.parseInt(airportInfo[ALTITUDE_INDEX]));
				airportNameCountry.set(airportInfo[AIRPORT_NAME_INDEX] + ", " + airportInfo[COUNTRY_INDEX]);
				context.write(altitude, airportNameCountry);
			}
		}
	}
	
	
	public static class FindHighestAirportReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		
		/**
		 * Airports are group by altitudes.
		 * The last entry is the highest airport
		 * @param key altitude
		 * @param values airport names and countries <name, country>
		 */
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text airportNameCountry : values) {
				context.write(key, airportNameCountry);
			}
		}
	}
}
