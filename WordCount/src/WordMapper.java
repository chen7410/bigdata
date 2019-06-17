import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
	 * 
	 * @author matt
	 * ShortWritable - input key type
	 * Text - input value type
	 * Text - output key type
	 * IntWritable - output value type
	 *
	 */
	public class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		/**
		 * convert the the whole text in to key-value pair. mapping
		 * @param key input key
		 * @param value input value, one line of the text
		 * 
		 */
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			final String line = value.toString();
			String[] words = line.split(" ");
			Text outputKey;
			for (String word: words) {
//				word = word.replaceAll("[^\\p{L}\\p{Nd}]+", "");
				word = word.replaceAll("[^a-zA-Z]+", "");
//				System.out.println(word);
				outputKey = new Text(word.toUpperCase());
				IntWritable outputValue = new IntWritable(1);
				//generate an output key-vale pair
				context.write(outputKey, outputValue);
			}
		}
	}