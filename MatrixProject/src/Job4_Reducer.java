import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configuration;

import weka.classifiers.AggregateableEvaluation;
import weka.classifiers.Classifier;
import weka.classifiers.bayes.BayesNet;
import weka.classifiers.bayes.NaiveBayes;
import weka.classifiers.lazy.IBk;
import weka.classifiers.lazy.KStar;
import weka.classifiers.lazy.LWL;
import weka.classifiers.rules.DecisionTable;
import weka.classifiers.rules.OneR;
import weka.classifiers.rules.PART;
import weka.classifiers.rules.ZeroR;
import weka.classifiers.trees.DecisionStump;
import weka.classifiers.trees.J48;
import weka.classifiers.trees.REPTree;
import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instances;
import weka.core.SparseInstance;

import com.google.inject.Key;

import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import weka.classifiers.AggregateableEvaluation;
import weka.classifiers.Evaluation;


public class Job4_Reducer extends Reducer <IntWritable,Text,IntWritable,Text> {

	static String reducerClassifierStr = null, outputPath = null;
	static Classifier reducerClassifier = null;
	static int reducerNum = 0, totalFeatures = 0, tempCount = 0;
	AggregateableEvaluation eval = null, tempEval = null;

	static Instances dataset = null, tempSet = null;

	FastVector<Attribute> fvWekaAttributes = new FastVector<Attribute>();


	public void setup(Context context) {
		TaskAttemptID tid = context.getTaskAttemptID();		
		String[] splitter = tid.toString().split("_");
		reducerNum = Integer.parseInt(splitter[4]);

		System.out.println("\n******** Processing Job 4 Reducer: "+reducerNum+ " ********\n");

		Configuration conf = context.getConfiguration();
		outputPath = conf.get("modelsPath");
		totalFeatures = Integer.parseInt(conf.get("totalFeatures"));
		reducerClassifierStr = conf.get("parClassifiers");
		reducerClassifierStr = getReducerClassifierName(reducerClassifierStr);

		try{
			System.out.println("Reading in model for Testing...." );
			reducerClassifier = (Classifier) weka.core.SerializationHelper.read(outputPath+"Models/"+reducerNum+"_"+reducerClassifierStr+".model");
			System.out.println("Model ***" + reducerClassifier.getClass().getSimpleName() + " ***loaded succesfully!");
		}catch(Exception e){
			e.printStackTrace();
		}
		/*
		 * Creates Array of attributes to make into the instance data
		 */
		for(int k =0; k < totalFeatures; k++)
			fvWekaAttributes.addElement(new Attribute("Feature "+ k));


		FastVector<String> fvNominalVal = new FastVector<String>(2);
		fvNominalVal.addElement("Rec.Autos");
		fvNominalVal.addElement("talk.politics.mideast");

		fvWekaAttributes.addElement( new Attribute("Class Attribute", fvNominalVal));
		try{
			dataset = new Instances("FeatureInstance",fvWekaAttributes,fvWekaAttributes.size()-1); 	
			dataset.setClassIndex(fvWekaAttributes.size()-1);
				System.out.println(dataset.classAttribute().toString());
			
			eval = new AggregateableEvaluation(dataset);

		}catch(Exception e){
			e.printStackTrace();
		}



	}

	public void reduce(IntWritable key, Iterable<Text> values, Context context)throws IOException , InterruptedException{

		System.out.println("Reducer Key" + key.toString());

		String[] lines = null, splitLine = null; 
		String instanceClass = null;

		double[] InstanceValues = null;
		int[] InstanceIndices = null;

		dataset.setClassIndex(fvWekaAttributes.size()-1);


		for(Text val:values){
			tempCount++;
			lines = val.toString().split("\n");
			splitLine = lines[0].split("\t");
			instanceClass = splitLine[1];

			lines[0] = "0";
			Arrays.sort(lines, new Comparator<String>() {
				@Override
				public int compare(String o1, String o2) {
					String[] splitStr = o1.split("\t");
					int index1 = Integer.parseInt(splitStr[0]);
					splitStr = o2.split("\t");
					int index2 = Integer.parseInt(splitStr[0]);
					if(index1 < index2)
						return -1;
					if(index2 > index1)
						return 1;
					else return 0;
				}
			});
			
			InstanceValues = new double[lines.length-1];
 			InstanceIndices = new int[lines.length-1];
 
 			for(int k = 1; k < lines.length; k++){
 				splitLine = lines[k].split("\t");
 				InstanceIndices[k-1] = Integer.parseInt(splitLine[0]);
 				InstanceValues[k-1] = Double.parseDouble(splitLine[1]);
 			}
 
 			/*
 			 * Builds Instance Row From the Value in the Loop
 			 */
 			SparseInstance instanceRow = new SparseInstance(1.0,InstanceValues,InstanceIndices,totalFeatures + 1);
 			if(instanceClass.equals("rec.autos"))
 				instanceRow.setValue(fvWekaAttributes.size()-1, 0.5);
 			else	
 				instanceRow.setValue(fvWekaAttributes.size()-1, 1.0);
			
 			dataset.add(instanceRow);	
 		
 		//	context.write(new IntWritable(key.get()), new Text(val.toString()));
 			
		}

	//	if(reducerNum == 0)
	//	System.out.println(dataset.toString());
		System.out.println(tempCount);

	}



	protected void cleanup(Context context) throws IOException, InterruptedException {

	//	System.out.println("\nCount" + count);

	}

	private String getReducerClassifierName(String par){
		String splitter[] = par.split(" ");
		return splitter[reducerNum];

	}
}