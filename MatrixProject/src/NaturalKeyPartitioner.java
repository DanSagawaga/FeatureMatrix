import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Partitions key based on "natural" key of {@link StockKey} (which
 * is the symbol).
 * @author Jee Vang
 *
 */
public class NaturalKeyPartitioner extends Partitioner<StockKey, DoubleWritable> {

	@Override
	public int getPartition(StockKey key, DoubleWritable val, int numPartitions) {
		int hash = key.getSymbol().hashCode();
		int partition = hash % numPartitions;
		return partition;
	}

}