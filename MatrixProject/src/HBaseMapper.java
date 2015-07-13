import java.io.BufferedReader;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/* 
 * To define a map function for your MapReduce job, subclass 
 * the Mapper class and override the map method.
 * The class definition requires four parameters: 
 *   The data type of the input key
 *   The data type of the input value
 *   The data type of the output key (which is the input key type 
 *   for the reducer)
 *   The data type of the output value (which is the input value 
 *   type for the reducer)
 */

public class HBaseMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  /*
   * The map method runs once for each line of text in the input file.
   * The method receives a key of type LongWritable, a value of type
   * Text, and a Context object.
   */
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  
	    HBaseManager HBaseAdmin = null;
	    HBaseAdmin= new HBaseManager();
		HBaseAdmin.pickTable("DanTestTable");
        String curLine = value.toString();

    
	StringTokenizer tokenizer = null;
	String feature = "";


	try{
			tokenizer = new StringTokenizer(curLine,"\t");
			if(tokenizer.hasMoreTokens())
				feature = tokenizer.nextToken();
			else{
				System.out.println("Bad DocFreq Feature "  + feature);
				feature = "";
			}
			
	        context.write(new Text(feature), new IntWritable(1));

	}catch(Exception e){
		System.out.println("ERROR: Reading in totalDocs files: " + e.getMessage());
	}
    
    
    
  }
}