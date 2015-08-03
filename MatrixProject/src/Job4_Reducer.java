import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

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


public class Job4_Reducer extends Reducer <IntWritable,Text,IntWritable,Text> {
	//	MultipleOutputs<Text, DoubleWritable> mos;
	static Instances dataset = null;
	static int totalFeatures = 0;
	static ArrayList<Attribute> attributes = new ArrayList<Attribute>();
	FastVector<Attribute> fvWekaAttributes = new FastVector<Attribute>();

	static Classifier[] models = { new J48(),new PART(),new DecisionTable(),new DecisionStump()
	,new J48(), new J48(),new KStar(),new ZeroR(),new OneR(),new REPTree()};
	AggregateableEvaluation[] evals = new AggregateableEvaluation[10];
	AggregateableEvaluation tempEval = null;

	MultipleOutputs<Text, Text> mos;




	public void setup(Context context) {
		System.out.println("\n******** Processing Job 4 Reducer ********\n");
		Configuration conf = context.getConfiguration();
		totalFeatures = Integer.parseInt(conf.get("totalFeatures"));
		String outputPath = conf.get("modelsPath");
		/*
		 * Creates Array of attributes to make into the instance data
		 */
		for(int k =0; k < totalFeatures + 1 ; k++)
			fvWekaAttributes.addElement(new Attribute("feature "+ k));

			//attributes.add(new Attribute(""+k));

		ArrayList<String> classifierList = new ArrayList<String>();
		classifierList.add("Rec.Autos");
		classifierList.add("talk.politics.mideast");
		//attributes.add(new Attribute("Classifiers",classifierList));	
		FastVector fvNominalVal = new FastVector(2);
		 fvNominalVal.addElement("Rec.Autos");
		 fvNominalVal.addElement("talk.politics.mideast");
		 //attributes.add( new Attribute("Document Class", fvNominalVal));
		 fvWekaAttributes.addElement( new Attribute("Document Class", fvNominalVal));

		try{
			dataset = new Instances("FeatureInstance",fvWekaAttributes,totalFeatures+1); 		
			dataset.setClassIndex(fvWekaAttributes.size()-1);
			System.out.println(dataset.classAttribute().toString());
			System.out.println(dataset.toSummaryString());

		//	dataset = new Instances("FeatureInstance",attributes,totalFeatures+1);
		//	dataset.setClassIndex(totalFeatures+1);

			System.out.println("\nReading in Classifiers and Models\n");
			//instantiates evaluation objects
			for(int k=0; k < evals.length; k++){
				models[k] = (Classifier) weka.core.SerializationHelper.read(outputPath+"Models/"+k+".model");
				evals[k] = new AggregateableEvaluation(dataset);
				//evals[k] = (AggregateableEvaluation) weka.core.SerializationHelper.read(outputPath+"Evaluations/"+k+".evaluation");
				//System.out.println("Classifier: "+k+" ACCURACY: "+evals[k].pctCorrect()+"%");
			}
			models[1] = (PART) weka.core.SerializationHelper.read(outputPath+"Models/"+1+".model");
			tempEval = new 	AggregateableEvaluation(dataset);
			System.out.println("\nRead in Classifiers and Models Sucessfully\n");

		}catch(Exception e){
			e.printStackTrace();
		}

	}

	public void reduce(IntWritable key, Iterable<Text> values, Context context)throws IOException , InterruptedException{

		//System.out.println(Key.get());

		String[] lines = null, splitLine = null; 
		String DocID = null, instanceClass = null, featureStr = "";

		double[] InstanceValues = null;
		int[] InstanceIndices = null;

		SparseInstance sparseRow = null;

		dataset.setClassIndex(totalFeatures+1);
		//System.out.println(dataset.classIndex());

		for (Text val : values) {

			lines = val.toString().split("\n");
			splitLine = lines[0].split("\t");
			instanceClass = splitLine[1];

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
				instanceRow.setValue(totalFeatures+1, 0.5);
			else	
				instanceRow.setValue(totalFeatures+1, 1.0);

			//		System.out.println(instanceRow.toString());

			dataset.add(instanceRow);	

		}
		/*
		 * End of Iterable For loop 
		 */
		try{
			for(int k = 0; k < models.length; k++){

				if(k != key.get()){	//skips the classifier that trained on that fold from evaluating
					tempEval = new AggregateableEvaluation(dataset);
					tempEval.evaluateModel(models[k],dataset);
					evals[k].aggregate(tempEval);
					System.out.println("Classifier # " +(k)+ " | Evaluating fold: " +(key.get() +" | On "+dataset.numInstances()+" instances | "+" | ACCURACY: "+evals[k].pctCorrect()+"%"));
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
		System.out.println("\nWriting out Classifier and Evaluations\n");
		Configuration conf = context.getConfiguration();
		String outputPath = conf.get("modelsPath");
		try{
			for(int k = 0; k < models.length; k++){
				System.out.println("Classifier: "+k+" ACCURACY: "+evals[k].pctCorrect()+"%");
				weka.core.SerializationHelper.write(outputPath+"Models/"+k+".model", models[k]);
				weka.core.SerializationHelper.write(outputPath+"Evaluations/"+k+".evaluation", evals[k]);

			}
			System.out.println("\nWrote out Classifiers and Evaluations sucessfully\n");
		}catch(Exception e){
			e.printStackTrace();
		}
	}

}

//	context.write(new IntWritable(DocID.get()), new Text(featureList));
//System.out.println("DocID: " +DocID.toString()+"\n"+ featureList);
//	context.getCounter(Job3_Reducer_Counter.LINES).increment(1);//increments the counter so it can be used as indexer
//	System.out.println(context.getCounter(Job3_Reducer_Counter.LINES).getValue());

//	if(context.getCounter(Job3_Reducer_Counter.LINES).getValue() < totalDocuments/10)
//		System.out.println("First Document: " +context.getCounter(Job3_Reducer_Counter.LINES).getValue());