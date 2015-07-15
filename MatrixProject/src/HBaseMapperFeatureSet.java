import java.io.IOException;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class HBaseMapperFeatureSet  extends Mapper<LongWritable, Text, ImmutableBytesWritable, Mutation> { // co ImportFromFile-2-Mapper Define the mapper class, extending the provided Hadoop class.

	public enum Counters { LINES }

    public void map(LongWritable offset, Text line, Context context) throws IOException {
    	
    	System.out.println( line.toString() + "\n");
    }
}
