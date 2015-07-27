import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Stock key. This key is a composite key. The "natural"
 * key is the primaryKey. The secondary sort will be performed
 * against the secondaryKey.
 * @author Jee Vang
 *
 */
public class CompositeKey implements WritableComparable<CompositeKey> {

	private String primaryKey;
	private Double secondaryKey;
	
	/**
	 * Constructor.
	 */
	public CompositeKey() { }
	
	/**
	 * Constructor.
	 * @param primaryKey Stock primaryKey. i.e. APPL
	 * @param secondaryKey SecondaryKey. i.e. the number of milliseconds since January 1, 1970, 00:00:00 GMT
	 */
	public CompositeKey(String primaryKey, Double secondaryKey) {
		this.primaryKey = primaryKey;
		this.secondaryKey = secondaryKey;
	}
	
	@Override
	public String toString() {
		return (new StringBuilder())
				.append('{')
				.append(primaryKey)
				.append(',')
				.append(secondaryKey)
				.append('}')
				.toString();
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		primaryKey = WritableUtils.readString(in);
		secondaryKey = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, primaryKey);
		out.writeDouble(secondaryKey);
	}

	@Override
	public int compareTo(CompositeKey o) {
		int result = primaryKey.compareTo(o.primaryKey);
		if(0 == result) {
			result = secondaryKey.compareTo(o.secondaryKey);
		}
		return result;
	}

	/**
	 * Gets the primaryKey.
	 * @return PrimaryKey.
	 */
	public String getPrimaryKey() {
		return primaryKey;
	}

	public void setPrimaryKey(String primaryKey) {
		this.primaryKey = primaryKey;
	}

	/**
	 * Gets the secondaryKey.
	 * @return SecondaryKey. i.e. the number of milliseconds since January 1, 1970, 00:00:00 GMT
	 */
	public Double getSecondaryKey() {
		return secondaryKey;
	}

	public void setSecondaryKey(Double secondaryKey) {
		this.secondaryKey = secondaryKey;
	}

}
