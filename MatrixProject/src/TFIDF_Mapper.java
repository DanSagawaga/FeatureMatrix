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
import java.util.*;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;





public class TFIDF_Mapper extends TableMapper<ImmutableBytesWritable, Put>  {

	public void map(ImmutableBytesWritable rows, Result columns, Context context) throws IOException, InterruptedException {

		HashMap<byte[], byte[]> IDF_Map = new HashMap<byte[], byte[]>(); // <String feature, String index + docFreq>

		
	//	System.out.println(Bytes.toString(columns.getRow()));
	//	System.out.println(("Start " + columns.toString()));
		
		
		try{
		String keyValueRow = "";
		   for (KeyValue keyValue : columns.raw()) {
			   
			   keyValueRow = Bytes.toString(keyValue.getRow());
			   
			   if(keyValueRow.equals("IDF_Row"))	 {  
				   IDF_Map.put(keyValue.getQualifier(), keyValue.getValue());		   
				System.out.println(Bytes.toString(keyValue.getQualifier()) + "     " + Bytes.toString(keyValue.getValue()));
			   }
			}
		   
		   //creating new keyValue with updated value
		   for (KeyValue inKeyValue : columns.listKeyValues()) {
			   keyValueRow = Bytes.toString(inKeyValue.getRow());
			   if(!keyValueRow.equals("IDF_Row") && !(keyValueRow.equals("Index_Row"))){  
				   //computes the TFxIDF and writes it to a new keyValue
				   double TFxIDF = Double.parseDouble(Bytes.toString(inKeyValue.getValue()));
				   
				   KeyValue outKeyValue = KeyValueUtil.createKeyValue(inKeyValue.getRow(),inKeyValue.getFamily(),inKeyValue.getQualifier());

	//***************   KeyValue(byte[] row, byte[] family, byte[] qualifier, byte[] value)
				//  Constructs KeyValue structure filled with null value
				   
		   }
		   }
		   
		
		}
		catch(Exception e){
			e.printStackTrace();
		}
   	}

  	private static Put resultToPut(ImmutableBytesWritable row, Result result) throws IOException {
  		Put put = new Put(row.get());
 		for (KeyValue kv : result.raw()) {
 			System.out.println(Bytes.toString(kv.getValue()));
			put.add(kv);
		}
		return put;
   	}
}

   	
   	
   	
   	
   /*	
Get get = new Get(Bytes.toBytes(rowPar));
		get.addFamily(Bytes.toBytes(familyNamePar));	   
		Result result1 = htable.get(get);  	  
		return "" +(Bytes.toDouble(result1.getValue(Bytes.toBytes(familyNamePar), Bytes.toBytes(columnPar))));
   	
   	
   	
   	
   	
   	
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
*/

