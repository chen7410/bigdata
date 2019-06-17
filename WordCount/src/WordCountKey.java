import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class WordCountKey implements WritableComparable<WordCountKey> {
	private String word;
	private int count;
	@Override
	public void readFields(DataInput in) throws IOException {
		word = in.readLine();
		count = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(count);
		out.writeBytes(word);
		
	}


	@Override
	public int compareTo(WordCountKey o) {
		// TODO Auto-generated method stub
		return 0;
	}

}
