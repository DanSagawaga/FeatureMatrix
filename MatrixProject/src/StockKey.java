import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Stock key. This key is a composite key. The "natural"
 * key is the symbol. The secondary sort will be performed
 * against the timestamp.
 * @author Jee Vang
 *
 */
public class StockKey implements WritableComparable<StockKey> {

	private String symbol;
	private Double timestamp;
	
	/**
	 * Constructor.
	 */
	public StockKey() { }
	
	/**
	 * Constructor.
	 * @param symbol Stock symbol. i.e. APPL
	 * @param timestamp Timestamp. i.e. the number of milliseconds since January 1, 1970, 00:00:00 GMT
	 */
	public StockKey(String symbol, Double timestamp) {
		this.symbol = symbol;
		this.timestamp = timestamp;
	}
	
	@Override
	public String toString() {
		return (new StringBuilder())
				.append('{')
				.append(symbol)
				.append(',')
				.append(timestamp)
				.append('}')
				.toString();
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		symbol = WritableUtils.readString(in);
		timestamp = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, symbol);
		out.writeDouble(timestamp);
	}

	@Override
	public int compareTo(StockKey o) {
		int result = symbol.compareTo(o.symbol);
		if(0 == result) {
			result = timestamp.compareTo(o.timestamp);
		}
		return result;
	}

	/**
	 * Gets the symbol.
	 * @return Symbol.
	 */
	public String getSymbol() {
		return symbol;
	}

	public void setSymbol(String symbol) {
		this.symbol = symbol;
	}

	/**
	 * Gets the timestamp.
	 * @return Timestamp. i.e. the number of milliseconds since January 1, 1970, 00:00:00 GMT
	 */
	public Double getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Double timestamp) {
		this.timestamp = timestamp;
	}

}
