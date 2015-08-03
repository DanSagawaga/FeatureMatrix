import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
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
	//	MultipleOutputs<Text, DoubleWritable> mos;
	static Instances dataset = null, tempSet = null;
	static int totalFeatures = 0;
	static FastVector<Attribute> fvWekaAttributes = new FastVector<Attribute>();

	static Classifier[] models = new Classifier[10];
	
	AggregateableEvaluation[] evals = new AggregateableEvaluation[10];
	Evaluation tempEval = null;

	MultipleOutputs<Text, Text> mos;



	public void setup(Context context) {
		System.out.println("\n******** Processing Job 4 Reducer ********\n");
		
		Configuration conf = context.getConfiguration();
		totalFeatures = Integer.parseInt(conf.get("totalFeatures"));
		String outputPath = conf.get("modelsPath");
		
		/*
		 * Creates Array of attributes to make into the instance data
		 */
		for(int k =0; k < totalFeatures; k++)
			fvWekaAttributes.addElement(new Attribute("Feature "+ k));

			//attributes.add(new Attribute(""+k));


		FastVector fvNominalVal = new FastVector(2);
		 fvNominalVal.addElement("Rec.Autos");
		 fvNominalVal.addElement("talk.politics.mideast");
		 
		 fvWekaAttributes.addElement( new Attribute("Document Class", fvNominalVal));

		try{
			dataset = new Instances("FeatureInstance",fvWekaAttributes,fvWekaAttributes.size()-1); 	
			tempSet = new Instances(dataset);

			dataset.setClassIndex(fvWekaAttributes.size()-1);
			System.out.println(dataset.classAttribute().toString());

			System.out.println("\nReading in Classifiers and Models\n");
			//instantiates evaluation objects
			for(int k=1; k < evals.length; k++){
				//models[k] = (Classifier) weka.core.SerializationHelper.read(outputPath+"Models/"+k+".model");
				models[k] = (Classifier) weka.core.SerializationHelper.read("/home/cloudera/Desktop/tempModels/"+k+".model");
			}
			
			models[0] = (J48) weka.core.SerializationHelper.read(outputPath+"Models/"+0+".model");
			
			tempEval = new 	AggregateableEvaluation(dataset);
			System.out.println("\nRead in Classifiers and Models Sucessfully\n");

		}catch(Exception e){
			e.printStackTrace();
		}

	}

	public void reduce(IntWritable key, Iterable<Text> values, Context context)throws IOException , InterruptedException{

		//System.out.println(Key.get());

		String[] lines = null, splitLine = null; 
		String instanceClass = null;

		double[] InstanceValues = null;
		int[] InstanceIndices = null;


		dataset.setClassIndex(fvWekaAttributes.size()-1);
		//System.out.println(dataset.classIndex());

		for (Text val : values) {

			lines = val.toString().split("\n");
			splitLine = lines[0].split("\t");
			instanceClass = splitLine[1];
	
			lines[0] = "-1";
			
			Arrays.sort(lines, new Comparator<String>() {
				@Override
				public int compare(String par1, String par2) {
					int index1 = 0, index2 = 0;
					String[] splitter = par1.split("\t");
					index1 = Integer.parseInt(splitter[0]);
					splitter = par2.split("\t");
					index2 = Integer.parseInt(splitter[0]);
					return Integer.valueOf(index1).compareTo(Integer.valueOf(index2));
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
			SparseInstance instanceRow = new SparseInstance(1.0,InstanceValues,InstanceIndices,fvWekaAttributes.size()-1);
			if(instanceClass.equals("rec.autos"))
				instanceRow.setValue(fvWekaAttributes.size()-1, 0.5);
			else	
				instanceRow.setValue(fvWekaAttributes.size()-1, 1.0);

			//		System.out.println(instanceRow.toString());

			dataset.add(instanceRow);	
		//	tempSet.add(instanceRow);	


		}
		/*
		 * End of Iterable For loop 
		 */
		try{
			for(int k = 0; k < models.length; k++){

				if(k != key.get()){	//skips the classifier that trained on that fold from evaluating
					tempEval = new Evaluation(dataset);
					tempEval.evaluateModel(models[k],dataset);
					if(evals[k] == null)
						evals[k] = new AggregateableEvaluation(tempEval);
					else 
					evals[k].aggregate(tempEval);
					
					System.out.println("Classifier # " +(k)+ " | Evaluating fold: " +(key.get() +" | On "+dataset.numInstances()+" instances | "+" | ACCURACY: "+tempEval.pctCorrect()+"%"));
					tempEval = null;
				}
			}
			dataset.delete();

			//System.out.println("Buidling Classifier: " + key.get() + " on "+dataset.numInstances()+" instances.");

			//	System.out.println();

		}catch (Exception e){
			e.printStackTrace();
		}


		System.out.println();
	}


	protected void cleanup(Context context) throws IOException, InterruptedException {
		
	//	System.out.println(tempSet.toString());
		
		System.out.println("\nWriting out Classifier and Evaluations\n");
		Configuration conf = context.getConfiguration();
		String outputPath = conf.get("modelsPath");
		try{
			for(int k = 0; k < models.length; k++){
				System.out.println("Classifier: "+k+" ACCURACY: "+evals[k].pctCorrect()+"%");
				weka.core.SerializationHelper.write(outputPath+"Evaluations/"+k+".evaluation", evals[k]);

			}
			System.out.println("\nWrote out Classifiers and Evaluations sucessfully\n");
		}catch(Exception e){
			e.printStackTrace();
		}
	}

}