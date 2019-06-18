import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FindAirportInCountry {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Find airport");
		job.setJarByClass(FindAirportInCountry.class);
		job.setMapperClass(FindAirportInCountryMapper.class);
		job.setReducerClass(FindAirportInCountryReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		
	}
	
	public static class FindAirportInCountryMapper extends Mapper<LongWritable, Text, Text, Text> {
		private static final String COUNTRY = "India";
		private Text outputKey = new Text(COUNTRY);
		private Text outputVal = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] airLineInfo = value.toString().split(",");
			if (airLineInfo[3].equalsIgnoreCase(COUNTRY)) {
				outputVal.set(airLineInfo[0] + " " + airLineInfo[1]);
				context.write(outputKey, outputVal);
			}
		}
	}
	
	
	public static class FindAirportInCountryReducer extends Reducer<Text, Text, Text, Text> {
		private Text dummyKey = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text airportNameAndId : values) {
				context.write(dummyKey, airportNameAndId);
			}
		}
	}
}
