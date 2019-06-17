import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class SortReducer extends Reducer<Text, Text, Text, Text> {
	
	
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		//eg <word 20>
		for(Text value : values) {
			context.write(key, value);
		}
	}
}
