import java.io.IOException;

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

public class FindAirportInCountry {
	private static final String COUNTRY = "India";
	private static final int AIRLINE_ID_INDEX = 0;
	private static final int AIRLINE_NAME_INDEX = 1;
	private static final int COUNTRY_INDEX = 3;
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Find airport in country");
		job.setJarByClass(FindAirportInCountry.class);
		job.setMapperClass(FindAirportInCountryMapper.class);
		job.setReducerClass(FindAirportInCountryReducer.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		
	}
	
	public static class FindAirportInCountryMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		private Text country = new Text(COUNTRY);
		private Text idName = new Text();
		
		/**
		 * get the country, id, airport name from a line.
		 */
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] airLineInfo = value.toString().split(",");
			if (airLineInfo[COUNTRY_INDEX].equalsIgnoreCase(COUNTRY)) {
				idName.set(airLineInfo[AIRLINE_ID_INDEX] + "," + airLineInfo[AIRLINE_NAME_INDEX]);
				context.write(country, idName);
			}
		}
	}
	
	
	public static class FindAirportInCountryReducer extends Reducer<Text, Text, IntWritable, Text> {
		private IntWritable id = new IntWritable();
		private Text airportName = new Text();
		
		/**
		 * the output is id-name pair
		 * @param key country
		 * @param values id-name pairs
		 */
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text idName : values) {
				String[] str = idName.toString().split(",");
				id.set(Integer.parseInt(str[0]));
				airportName.set(str[1]);
				context.write(id, airportName);
			}
		}
	}
}
