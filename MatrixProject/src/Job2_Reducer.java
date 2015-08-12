import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/*
 * TD IDF Reducer
 * 
 * In this reducer the inverse document frequency values from the docFreq mapper and the term frequency values 
 * from the feature Set Mapper are grouped by their feature. With the use of secondary sorting, in each feature-key
 * iterable, the IDF value will come first and the multiple TF values will come after it. 
 * The TFxIDF is then computed sequentially in each iterable loop and written out in the form of 
 * Document ID, Feature index, TFxIDF value. 
 * This writes out the first format of the matrix that is further formatted in the next job to meet classifying requiremnts.
 * 
 * Another separate file is written containing a list of the feature names and their assigned index for testing and validating data.
 *  
 */
	public class Job2_Reducer extends Reducer<CompositeKey,Text,Text,DoubleWritable>{
		MultipleOutputs<Text, DoubleWritable> mos;

		long totalRecords = 0;
		long totalFeatures = 0;
		
		public void setup(Context context) {
			System.out.println("\n******** Processing TD_IDF_Reducer ********\n");
			mos = new MultipleOutputs<Text,DoubleWritable>(context);

		}

		public void reduce(CompositeKey key, Iterable<Text> valueList, Context context)throws IOException , InterruptedException{
			long recordCount = 0, featureCount = 0;
			String docID = null;
			double IDF = 0.0, TF = 0.0,TF_IDF = 0.0;
			long featureIndex = 0;

			Scanner scan = null;

			//	if(key.getSymbol().equals("11")){

			int count = 0;	
			for(Text value : valueList){
				//the first value in each list is the IDF value so it needs separate formatting.
				if(count == 0){
					scan = new Scanner(value.toString());
					scan.useDelimiter("\t");
					// The IDF_Flag string used as redundancy to makes sure the IDF value comes first
					if(scan.next().equals("IDF_Flag")){
						featureIndex = Long.parseLong(scan.next());
						IDF = Double.parseDouble(scan.next());
						//	System.out.println("IDF: "+ IDF);
						//Keeps count of the total amount of unique features in the data set for later use
						if(featureIndex > totalFeatures)
							featureCount = featureIndex;
					}
					else{
						System.out.println("Error in Reducer: IDF Key not found for feature: "+key.getPrimaryKey());
						break;
					}	
				}
				//The rest of the values are the Term Frequencies.
				//They are each multiplied by the IDF to get the TFxIDF each DocID x FeatureIndex Record
				else{
					scan = new Scanner(value.toString());
					scan.useDelimiter("\t");
					docID = scan.next();
					TF = Double.parseDouble(scan.next());
					TF_IDF = TF * IDF;
					//System.out.println("DocID: "+docID+" Feature: "+key.getSymbol()+" TF: "+TF+" IDF: "+IDF+" TF_IDF: "+TF_IDF);
					//	System.out.println("DocID: "+docID+" FeatureIndex: "+featureIndex+" Feature Name: "+key.getSymbol()+" TF_IDF: "+TF_IDF);
					mos.write("Seq",new Text(docID+"\t"+featureIndex),new DoubleWritable(TF_IDF));
					//context.write(new Text(docID+"\t"+featureIndex),new DoubleWritable(TF_IDF));
					recordCount++;
				}
				count++;
			}
			totalRecords += recordCount;
			totalFeatures = featureCount;
			mos.write("FeatureIndexKeyText",new Text(key.getPrimaryKey()+ "\t"+featureIndex),null);
		}
		
		//Closes the multipleOutput object stream 
		protected void cleanup(Context context) throws IOException, InterruptedException {
			context.write(new Text("\n"), new DoubleWritable(totalFeatures+ 0.0));
			context.write(new Text(""), new DoubleWritable(totalRecords+ 0.0));
			mos.close();
		}
	}