import java.io.IOException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import java.lang.Math;

public class HbaseDocFreqMapper  extends Mapper<Text, Text, Text, Text> { // co ImportFromFile-2-Mapper Define the mapper class, extending the provided Hadoop class.

    public enum Counters { LINES }

    protected void setup(Context context) throws IOException, InterruptedException {

    }
    
    @Override
    public void map(Text featureText, Text docFreqText, Context context) // co ImportFromFile-3-Map The map() function transforms the key/value provided by the InputFormat to what is needed by the OutputFormat.
    throws IOException {
      try {
          
          String feature = featureText.toString();
          String docFreqStr = docFreqText.toString();
          long numOfDocs = 203;
          String IDF = "";

          /*
           * Checks the input to the mapper for any null or empty values
           */
            if((feature == null || feature == "")||(docFreqStr == null || docFreqStr == ""))
                System.out.println("Bad feature from DocFreq File: # " + context.getCounter(Counters.LINES).getValue() +
                        "feature: " + feature);
            
            else{                
                /*
                 * The input for the Row will be the column index of the features in the matrix + their DocFreq to save space
                 * The Hadoop Mapper Counter will be used as the index value ect. order that they are written into hbase 
                 * 
                 * Inverse Term Frequency 'IDF' is calculated using the equation IDF = log (# of Documents/ document Frequency)
                 */
            	
            	IDF =""+ Math.log10(numOfDocs/Double.parseDouble(docFreqStr));

     
                context.write(new Text(feature), new Text("IDF_Flag " + IDF));
              //  System.out.println("Feature: " + feature + " Index: " + context.getCounter(Counters.LINES).getValue() + " IDF: " + IDF);

            }
           
           
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }