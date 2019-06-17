import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
	 * 
	 * @author matt
	 * Text - input key type, match the Mapper output key type.
	 * IntWritable - input value type, match the Mapper output value.
	 * Text - output key type
	 * IntWritable - output value type
	 */
	public class WordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		/**
		 * combining. group by key and sum the values of the key. 
		 * @param key input key.
		 * @param values a set of values.
		 */
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value: values) {
				sum += value.get();
			}
			//output key, output value
			context.write(key, new IntWritable(sum));
		}
	}