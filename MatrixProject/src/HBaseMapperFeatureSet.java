import java.io.IOException;
import java.io.StringReader;
import java.util.TreeMap;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.input.SAXBuilder;
import org.apache.hadoop.io.WritableComparable;


import java.util.*;



public class HBaseMapperFeatureSet extends Mapper<Text, Text, StockKey, Text> {  // co ImportFromFile-2-Mapper Define the mapper class, extending the provided Hadoop class.

	private static HashMap<String,Double> weightMap = new HashMap<String,Double>();
	
	public enum Counters { LINES }

	
    public void map(Text DocID, Text line, Context context) throws IOException {
    	
    	String xmlString = line.toString();
    	double tempWeight = 0, maxWeight = 0;
    	String docFreq = "", TF = "";
    	Put put = null;
    	
    	
		try {
			Document doc = new SAXBuilder().build(new StringReader(xmlString));
			if (doc!=null) {
				for (Element child : doc.getRootElement().getChildren()) {
					if (child.getName().equalsIgnoreCase("feature")) {
						tempWeight = Double.parseDouble(child.getAttribute("weight").getValue());
						if(tempWeight > maxWeight)
							maxWeight = tempWeight;
						
						weightMap.put(child.getAttribute("name").getValue(),tempWeight);
					}
				}
			}
     			
		    for (Map.Entry<String, Double> entry : weightMap.entrySet()) {
		        String feature = entry.getKey();
		        tempWeight = entry.getValue();
		        /*
		         * The Term frequency is calculated using the the equation tf(t,d) = .5 + (.5 * f(t,d) ) / maxWeight f(t,d)
		         */
		        TF ="" +(.5 + (.5 *tempWeight)/maxWeight);
		       // docFreq = new String(value.getValue(Bytes.toBytes("IndexRowTest1"), Bytes.toBytes(feature)));
		    //    System.out.println("Mapper Doc ID: " + DocID + " Feature: " + feature + " Term Freq: " + tempWeight/maxWeight);

		        
		      //  put = new Put(Bytes.toBytes(DocID.toString()));//sets the row of the hbase matrix to the documnet ID
			  //  put.addColumn(Bytes.toBytes("FeatureFamily"), Bytes.toBytes(feature), Bytes.toBytes(TF)); // co ImportFromFile-5-Put Store the original data in a column in the given table.
		        
			    context.write(new StockKey(feature, Double.parseDouble(DocID.toString())), new Text(DocID.toString() + "\t" + TF));	
			} 
		    weightMap.clear();

		}catch(Exception e){
			System.out.println("Error in the Mapper: "+e.getMessage());
		} 	
    }
    
    
}

