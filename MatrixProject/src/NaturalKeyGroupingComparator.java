import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Groups values based on symbol of {@link CompositeKey} (the natural key).
 * @author Jee Vang
 *
 */
public class NaturalKeyGroupingComparator extends WritableComparator {

	/**
	 * Constructor.
	 */
	protected NaturalKeyGroupingComparator() {
		super(CompositeKey.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		CompositeKey k1 = (CompositeKey)w1;
		CompositeKey k2 = (CompositeKey)w2;
		
		return k1.getPrimaryKey().compareTo(k2.getPrimaryKey());
	}
}
