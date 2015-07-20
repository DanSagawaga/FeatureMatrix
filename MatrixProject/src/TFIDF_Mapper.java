import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.*;
import java.util.NavigableMap;



public class TFIDF_Mapper extends TableMapper<Text, IntWritable>  {

	
	private final IntWritable ONE = new IntWritable(1);
   	private Text text = new Text();
   	HBaseManager admin = new HBaseManager();

   	public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
   		    Get get = new Get(Bytes.toBytes(rowPar));
        	String val = new String(value.getValue(Bytes.toBytes("cf"), Bytes.toBytes("attr1")));
          	text.set(val);     // we can only emit Writables...

        	context.write(text, ONE);
   	}
   	
   	
   	
   	
   	
	public void getRows(String ColumnFamily)throws IOException{
		Scan scan = new Scan();
		scan.setFilter(new FirstKeyOnlyFilter());
		ResultScanner scanner = htable.getScanner(scan);
		for (Result result : scanner)
			System.out.println("Row Key: " + Bytes.toInt(result.getRow()));	 
		scanner.close();
	}
   	
   	
   	
   	
   	
   	
	public String[] getColumns(String rowPar, String ColumnFamily) throws IOException	{
		Get get = new Get(Bytes.toBytes(rowPar));
		//get.addFamily(Bytes.toBytes());	   
		Result r = htable.get(get); 

		NavigableMap<byte[], byte[]> familyMap = r.getFamilyMap(Bytes.toBytes(ColumnFamily));
		String[] Quantifers = new String[familyMap.size()];

		int counter = 0;
		for(byte[] bQunitifer : familyMap.keySet()) {
			Quantifers[counter++] = Bytes.toString(bQunitifer);
		}

		return Quantifers;
	}

}
