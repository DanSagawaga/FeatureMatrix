import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Partitions key based on "natural" key of {@link CompositeKey} (which
 * is the symbol).
 * @author Jee Vang
 *
 */
public class NaturalKeyPartitioner extends Partitioner<CompositeKey, DoubleWritable> {

	@Override
	public int getPartition(CompositeKey key, DoubleWritable val, int numPartitions) {
		int hash = key.getPrimaryKey().hashCode();
		int partition = hash % numPartitions;
		System.out.println("Natural Partitioner");
		return partition;
	}

}