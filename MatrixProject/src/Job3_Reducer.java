import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/*
 * @author: Dan Saganome
 * 
 * Job 3 Reducer 
 * 
 * In this reducer the values are grouped by the Document ID as the Key. 
 * Because of secondary sorting, the class membership value comes first followed by the sorted set of features and their values. 
 * The final format of the matrix is written out as the document ID and class membership as the key,
 * and the list of its features and TFxIDF values as the values. 
 * 
 * A separate file is written containing each unique class membrship to pass as parameters for later jobs.
 * 
 */


public class Job3_Reducer extends Reducer<CompositeKey,Text,Text,Text>{

	static HashMap<String,String> classMap = new HashMap<String,String>();

	MultipleOutputs<Text, Text> mos;
	public enum Job3_Reducer_Counter { LINES }

	public void setup(Context context) {
		System.out.println("\n******** Processing Job 3 Reducer ********\n");
		//instantiates multiple output writer
		mos = new MultipleOutputs<Text,Text>(context);
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

		//Adds all Document classes to map so they can be written out for later use
		if(!classMap.containsKey(classifier))
			classMap.put(classifier,classifier);

		//checks for empty values
		if(featureList.toString().equals(""))
			System.out.println("Document: "+ DocID.getPrimaryKey()+ " has 0 features | Has been ommited from matrix.");
		else
			mos.write("FinalMatrixForm",new Text(DocID.getPrimaryKey()+"\t"+classifier), new Text(featureList));
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {

		//writes the separate text file containig each class membership
		for (String value : classMap.values()) {
			mos.write("DocumentClasses",new Text(value), new Text(""));
		}
		mos.close();
	}
}