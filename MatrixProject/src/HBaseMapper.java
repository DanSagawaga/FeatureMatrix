import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hbase.client.Put;





public class HBaseMapper  extends Mapper<LongWritable, Text, ImmutableBytesWritable, Mutation> { // co ImportFromFile-2-Mapper Define the mapper class, extending the provided Hadoop class.

	public enum Counters { LINES }

    // ^^ ImportFromFile
    /**
     * Prepares the column family and qualifier.
     *
     * @param context The task context.
     * @throws IOException When an operation fails - not possible here.
     * @throws InterruptedException When the task is aborted.
     */
    // vv ImportFromFile
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

    }
    
    @Override
    public void map(LongWritable offset, Text line, Context context) // co ImportFromFile-3-Map The map() function transforms the key/value provided by the InputFormat to what is needed by the OutputFormat.
    throws IOException {
      try {
          String lineString = line.toString(), feature = "", totalDocsWithFeature = "";
    	  
          StringTokenizer tokenizer = new StringTokenizer(lineString,"\t");
			if(tokenizer.hasMoreTokens())
				feature = tokenizer.nextToken();
			else{
				System.out.println("Bad DocFreq Feature " + feature);
				feature = "";
			}
			if(tokenizer.hasMoreTokens())
				totalDocsWithFeature = tokenizer.nextToken();
			else{
				System.out.println("Bad DocFreq Feeature: "  + "\n" + feature);
				totalDocsWithFeature = "";
			}
			
	       Put put = new Put(Bytes.toBytes("IndexRowTest1"));
	       put.addColumn(Bytes.toBytes("FeatureFamily"), Bytes.toBytes(feature), Bytes.toBytes(totalDocsWithFeature)); // co ImportFromFile-5-Put Store the original data in a column in the given table.
	       context.write(new ImmutableBytesWritable(Bytes.toBytes("IndexRowTest1")), put);
           context.getCounter(Counters.LINES).increment(1);
  
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }