import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.io.Text;
import java.lang.Math;

/*
 * Job 2 DocFreq Mapper
 * 
 * This Mapper reads in from a sequence file containing the Inverse Document frequency of all the features.
 * The features that are written out are assigned an index for the matrix using the mapper's internal counter (LINES)
 * A composite key is used as the output key for secondary sorting in the reducer.
 * The secondary key for all values is set to -1 so in the reducer, the inverse document frequency is the first in all the iterable lists.
 * This allows for the Inverse document frequency to be multiplied with the rest of values (Term Frequencies) in order to compute the TFxIDF
 * 
 */

public class Job2_DocFreq_Mapper  extends Mapper<Text, Text, CompositeKey, Text> {

	public enum Counters { LINES }
	int mapperNum = 0;
	long totalDocuments = 0;

	protected void setup(Context context) throws IOException, InterruptedException {
		TaskAttemptID tid = context.getTaskAttemptID();		
		String[] splitter = tid.toString().split("_");
		mapperNum = Integer.parseInt(splitter[4]);

		System.out.println("\n****************** Processing Job 2 DocFreq Mapper: "+mapperNum+ " ******************\n");

		Configuration conf = context.getConfiguration();
		totalDocuments = Long.parseLong(conf.get("totalDocuments"));

	}

	/*
	 * Simple line read containing the feature and its Inverse Document Frequency 
	 * The feature is used as the primary key and a -1 is used as the secondary key
	 */
	public void map(Text featureText, Text docFreqText, Context context) throws IOException, InterruptedException {


		String feature = featureText.toString();
		String docFreqStr = docFreqText.toString();
		String IDF = "";

		/*
		 * Checks the input to the mapper for any null or empty values
		 */
		if((feature == null || feature == "")||(docFreqStr == null || docFreqStr == ""))
			System.out.println("BAD FEATURE FORMAT: # "
					+ context.getCounter(Counters.LINES).getValue() +"feature: " + feature);

		else{                

			//Inverse Term Frequency 'IDF' is calculated using the equation IDF = log (# of Documents/ document Frequency)
			IDF =""+ Math.log10(totalDocuments/Double.parseDouble(docFreqStr));

			context.write(new CompositeKey(feature,-1.0),
					new Text("IDF_Flag"+"\t"+context.getCounter(Counters.LINES).getValue()+"\t"+IDF));
			context.getCounter(Counters.LINES).increment(1);//increments the counter so it can be used as indexer
			//  System.out.println("Feature: " + feature + " Index: " + context.getCounter(Counters.LINES).getValue() + " IDF: " + IDF)
		}
	}
}