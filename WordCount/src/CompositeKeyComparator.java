import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKeyComparator extends WritableComparator {
	
	/**
	 * Constructor.
	 */
	protected CompositeKeyComparator() {
		super(Text.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		String line1 = w1.toString();
		String line2 = w2.toString();
		String[] word1 = line1.split("\\s+");
		String[] word2 = line2.split("\\s+");
		
		return (Integer.parseInt(word1[1]) - (Integer.parseInt(word2[1]))) * (-1);
	}
}
