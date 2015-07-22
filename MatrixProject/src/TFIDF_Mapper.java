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
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValueUtil;





public class TFIDF_Mapper extends TableMapper<ImmutableBytesWritable, Put>  {

 //	private HashMap<String, String> IDF_Map = new HashMap<String, String>(); // <String feature, String index + docFreq>

	public void map(ImmutableBytesWritable rows, Result columns, Context context) throws IOException, InterruptedException {

		HashMap<String, String> IDF_Map = new HashMap<String, String>(); // <String feature, String index + docFreq>
		double TFxIDF = 0.0;
		KeyValue outKeyValue = null;

		
	//	System.out.println(Bytes.toString(columns.getRow()));
	//	System.out.println(("Start " + columns.toString()));
		
		
		try{
		String keyValue_Row = "";
		   for (KeyValue keyValue : columns.raw()) {
			   
			   keyValue_Row = new String(keyValue.getRow());
			   					   
			   if(keyValue_Row.equals("IDF_Row"))	 {  
				   IDF_Map.put(Bytes.toString(keyValue.getQualifier()), Bytes.toString(keyValue.getValue()));		   
			   }			   
			}
		   
		   
	//   if(IDF_Map.containsKey("$ 0.02"))
		//   System.out.println(IDF_Map.get("$ 0.02"));

			//System.out.println(IDF_Map.size());
   
		   //creating new keyValue with updated value
		   if(!IDF_Map.isEmpty()){
				System.out.println(IDF_Map.size() + " dfsdfsdf " + columns.size());

		   for (KeyValue keyValue : columns.raw()) {
			   keyValue_Row = Bytes.toString(keyValue.getRow());
			   
			 //  if(!keyValue_Row.equals("IDF_Row") && !(keyValue_Row.equals("Index_Row"))){  
				   //computes the TFxIDF and writes it to a new keyValue
				//	System.out.println(Bytes.toString(keyValue.getQualifier()));
					
				//		   if(IDF_Map.containsKey(Bytes.toString(keyValue.getQualifier())))
			  System.out.println(Bytes.toString(keyValue.getRow()) +"\t" + Bytes.toString(keyValue.getQualifier()) +"\t" + IDF_Map.get( Bytes.toString(keyValue.getQualifier())));
//	System.out.println(IDF_Map.size());
			   
/*				   if(IDF_Map.containsKey(Bytes.toString(keyValue.getQualifier()))){
				   TFxIDF = (Double.parseDouble(Bytes.toString(keyValue.getValue())) * Double.parseDouble((IDF_Map.get(keyValue.getQualifier()))));
				   outKeyValue =  new KeyValue(keyValue.getRow(),keyValue.getFamily(),keyValue.getQualifier(), Bytes.toBytes("" + TFxIDF));
				   
					System.out.println("Hello World");

					System.out.println(Bytes.toString(outKeyValue.getRow()) + "\t" +(Bytes.toString(outKeyValue.getQualifier())) + "     " + Bytes.toString(outKeyValue.getValue()));
				   }

	*/		   	
				   
				//System.out.println(Bytes.toString(outKeyValue.getRow()) + "\t" +(Bytes.toString(outKeyValue.getQualifier())) + "     " + Bytes.toString(outKeyValue.getValue()));

				   
		//   }
			   
			   
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

*/

/*
 * 
 * 		List<Cell> columnList = columns.getColumnCells(Bytes.toBytes("FeatureFamily"),Bytes.toBytes("# 007"));
		System.out.println(columnList.size() );

		
		if(!columnList.isEmpty()){
		Cell IDF_Cell = columnList.get(columnList.size() - 1);
	//	System.out.println("Last Cell value " +Bytes.toString(IDF_Cell.getValue()));
		}
		
		for(Cell currCell : columnList){
	//	System.out.println(Bytes.toString(currCell.getQualifier()) + "\t" + Bytes.toString(currCell.getValue()) );
		}
 * 
 * 
*/