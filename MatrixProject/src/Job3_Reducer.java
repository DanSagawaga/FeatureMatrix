import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;



public class Job3_Reducer extends Reducer<CompositeKey,Text,Text,Text>{

	int totalDocuments = 0, numFolds = 0, numDocsInFold =0, remainderCount = 0;
	long docCounter = 0;
	int[] foldDocCount = null;
	static HashMap<String,String> classMap = new HashMap<String,String>();

	MultipleOutputs<Text, Text> mos;
	public enum Job3_Reducer_Counter { LINES }

	public void setup(Context context) {
		System.out.println("\n******** Processing Job 3 Reducer ********\n");

		Configuration conf = context.getConfiguration();
		numFolds = Integer.parseInt(conf.get("numFolds"));
		totalDocuments = Integer.parseInt(conf.get("totalDocuments"));
		numDocsInFold = (int)(totalDocuments/numFolds);
		mos = new MultipleOutputs<Text,Text>(context);

		foldDocCount = new int[numFolds];
		for(int k = 0; k < numFolds; k++)
			foldDocCount[k] = 0;

	}


	public void reduce(CompositeKey DocID, Iterable<Text> values, Context context)throws IOException , InterruptedException{
		String classifier = "", featureList = "";
		int count = 0;
		for(Text value: values){
			if(count == 0){	//the first line holds the classifier, the rest are all the features
				classifier = value.toString();
				count++;
			}
			else
				featureList +=  value.toString() + "\n";
		}

		//	System.out.println("Document: "+ DocID.getPrimaryKey());

		//Adds all Documnet classes to map so they can be written out for later use
		if(!classMap.containsKey(classifier))
			classMap.put(classifier,classifier);

		if(featureList.toString().equals("")){
			System.out.println("Document: "+ DocID.getPrimaryKey()+ " has 0 features | Has been ommited from matrix.");
		}
		/*
		 * Writes n (number of Folds) matrices
		 */
		else{
				mos.write("FinalMatrixForm",new Text(DocID.getPrimaryKey()+"\t"+classifier), new Text(featureList));

		}
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {

		for (String value : classMap.values()) {
			mos.write("DocumentClasses",new Text(value), new Text(""));
		}
		mos.close();
		
	}
}