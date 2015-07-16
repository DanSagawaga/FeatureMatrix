import java.io.IOException;
import java.io.StringReader;
import java.util.TreeMap;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
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
import java.util.*;



public class HBaseMapperFeatureSet extends Mapper<Text, Text, ImmutableBytesWritable, Mutation> { // co ImportFromFile-2-Mapper Define the mapper class, extending the provided Hadoop class.

	private static HashMap<String,Double> weightMap = new HashMap<String,Double>();
	
	public enum Counters { LINES }

    public void map(Text DocID, Text line, Context context) throws IOException {
    	
    	//System.out.println( offset.toString() + "\n");    
    	String xmlString = line.toString();
    	double tempWeight = 0, maxWeight = 0;
	    Put put = new Put(Bytes.toBytes(DocID.toString()));

    	  	
		try {
			Document doc = new SAXBuilder().build(new StringReader(xmlString));
			if (doc!=null) {
				for (Element child : doc.getRootElement().getChildren()) {
					if (child.getName().equalsIgnoreCase(FeatureSet.XML_TAG_FEATURE)) {
						tempWeight = Double.parseDouble(child.getAttribute("weight").getValue());
						if(tempWeight > maxWeight)
							maxWeight = tempWeight;
						
						weightMap.put(child.getAttribute(FeatureSet.XML_TAG_FEATURE_NAME).getValue(),tempWeight);
					}
				}
			}
			
		    for (Map.Entry<String, Double> entry : weightMap.entrySet()) {
		        String key = entry.getKey();
		        tempWeight = entry.getValue();
		        System.out.println("Key: " + key + " weight: " + tempWeight/maxWeight);
		        
		        Get get = new Get(Bytes.toBytes("IndexRowTest1"));
				get.addFamily(Bytes.toBytes("FeatureFamily"));	   
				Result result1 = htable.get(get);  	  
				return (Bytes.toString(result1.getValue(Bytes.toBytes(familyNamePar), Bytes.toBytes(columnPar))));
			}
		        
			       put.addColumn(Bytes.toBytes("FeatureFamily"), Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()/maxWeight)); // co ImportFromFile-5-Put Store the original data in a column in the given table.
			    //   System.out.println("#: " + counterIndex + " Feature: " + feature );
			       context.write(new ImmutableBytesWritable(Bytes.toBytes(DocID.toString())), put);
		    }
		    
			
			
		}catch(Exception e){
			System.out.println("Error in the Mapper: "+e.getMessage());
		} 	
    }
}
