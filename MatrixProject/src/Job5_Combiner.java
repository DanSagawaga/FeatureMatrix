import java.io.*;
import java.io.IOException;
import java.util.*;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;


import weka.core.Instances;
import weka.classifiers.AggregateableEvaluation;
import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.trees.DecisionStump;
import weka.classifiers.trees.J48;
import weka.classifiers.rules.*;
import weka.classifiers.bayes.*;
import weka.classifiers.lazy.*;
import weka.core.SparseInstance;
import weka.core.Attribute;
import weka.classifiers.trees.RandomTree; 
import weka.classifiers.rules.ZeroR ;
import weka.classifiers.trees.REPTree;
import weka.core.FastVector;

public class Job5_Combiner extends Reducer<Text,Text,Text,Text> {

	static String[] classifiersToEvalNames = null;
	static String docClasses = null, outputPathStr= null;
	static Classifier[] classifierModels = null;
	ConfusionMatrix[] confMatrices = null;
	static int combinerNum = 0, totalFeatures = 0, numClasses = 0;
	AggregateableEvaluation[] eval = null;
	Evaluation tempEval = null;

	static Instances dataset = null, tempSet = null;

	FastVector<Attribute> fvWekaAttributes = new FastVector<Attribute>();
	FastVector<String> classNominalVal = null;
	HashMap<String,Double> classNominalMap = null;


	public void setup(Context context){

		TaskAttemptID tid = context.getTaskAttemptID();		
		String[] splitter = tid.toString().split("_");
		combinerNum = Integer.parseInt(splitter[4]);

		System.out.println("\n****************** Processing Job 5 Combiner: "+combinerNum+ " ******************\n");

		Configuration conf = context.getConfiguration();
		docClasses = conf.get("docClasses");
		numClasses = Integer.parseInt(conf.get("numClasses"));
		totalFeatures = Integer.parseInt(conf.get("totalFeatures"));
		outputPathStr = conf.get("modelsPath");
		classifiersToEvalNames = conf.get("parClassifiers").split("\t");
		loadClassifiers(classifiersToEvalNames, context);
		/*
		 * Creates Array of attributes to make into the instance data
		 */
		for(int k =0; k < totalFeatures; k++)
			fvWekaAttributes.addElement(new Attribute("Feature "+ k));


		splitter = docClasses.split("\n");
		classNominalVal = new FastVector<String>(splitter.length);
		classNominalMap = new HashMap<String,Double>(splitter.length);

		/*
		 * populates FastVector list to add as class attribute to the dataset
		 * populates HashMap to compare each document's class 
		 */
		for(int k = 0; k < splitter.length; k++){
			classNominalVal.addElement(splitter[k].trim());
			classNominalMap.put(splitter[k].trim(), k+0.1);
			//	System.out.println(classNominalMap.get(splitter[k].trim()));
		}


		fvWekaAttributes.addElement( new Attribute("Class Attribute", classNominalVal));
		try{
			dataset = new Instances("FeatureInstance",fvWekaAttributes,fvWekaAttributes.size()-1); 	
			dataset.setClassIndex(fvWekaAttributes.size()-1);
			System.out.println(dataset.classAttribute().toString());

			eval = new AggregateableEvaluation[classifiersToEvalNames.length];

			for(int k = 0; k < classifierModels.length; k++)
				eval[k] = new AggregateableEvaluation(dataset);


		}catch(Exception e){
			e.printStackTrace();
		}



	}


	public void reduce(Text key, Iterable<Text> values, Context context)throws IOException , InterruptedException{

		int tempCount = 0;

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

			instanceRow.setValue(fvWekaAttributes.size()-1, classNominalMap.get(instanceClass));
			
			dataset.add(instanceRow);
			try{
				for(int k = 0; k < classifierModels.length; k++){
					tempEval = new Evaluation(dataset);
					tempEval.evaluateModel(classifierModels[k], dataset);
					eval[k].aggregate(tempEval);
				}
				dataset.delete();
			}catch(Exception e){
				e.printStackTrace();
			}

		}

	}


	protected void cleanup(Context context) throws IOException, InterruptedException {

		try{

			loadConfusionMatrices();

			for(int k = 0; k < classifierModels.length; k++){
				//System.out.println("Evaluation Accurarcy for Model: " + classifierModels[k].getClass().getSimpleName() + " | "+ eval[k].pctCorrect());
				System.out.print("Evaluation Accuracy for Model: " + classifierModels[k].getClass().getSimpleName());
				//for(int j = 0; j < classNominalVal.size(); j++)
					System.out.print(" | " +eval[k].pctCorrect());
				System.out.println();
				
				context.write(new Text(classifierModels[k].getClass().getSimpleName()),
						new Text("Training Fold " +combinerNum+"\n"+confMatrices[k].getF1Scores() +"\n"+confMatrices[k].xml()+"\n"));
			//	System.out.println("Classifier and F scores: "+classifierModels[k].getClass().getSimpleName()+" | " +confMatrices[k].getF1Scores());
			}

		}catch(Exception e){
			e.printStackTrace();
		}

	}
	
	

	/*
	 * Parameter: String[] of the names of the classifiers to be evaluated
	 * Each String contains the classifier name and their options so they need 
	 * split in order to just use the classifier name to load the classifiers 
	 * externally.
	 */
	private void loadClassifiers(String[] classifiersPar, Context context){
		
		
		Configuration conf = context.getConfiguration();
		Path outputPath = new Path(outputPathStr+"Models/Mapper_"+combinerNum) ;	
		String[] splitter = null;
		System.out.println("Reading in model for Testing...." );
		
		try{
			FileSystem fs = FileSystem.get(conf);

			classifierModels = new Classifier[classifiersPar.length];
			for(int k = 0; k < classifiersPar.length;k++){
				splitter = classifiersPar[k].split(",");

				//fs = FileSystem.get(new Path(outputPath.toUri()+"/"+splitter[0].trim()+".model").toUri(), conf);
				//System.out.println(outputPath.toUri() +"/"+splitter[0].trim()+".model");
				FSDataInputStream in = fs.open(new Path(outputPath.toUri() +"/"+splitter[0].trim()+".model"));
				classifierModels[k] = (Classifier) weka.core.SerializationHelper.read(in);//(outputPath.toUri() +"/"+splitter[0].trim()+".model");
			}

		}catch(Exception e){
			System.out.println("ERROR: Could not read in classifier model files into Combiner: " + combinerNum);
			e.printStackTrace();
		}
		System.out.println("Read in models succesfully!\n");
	}
	/*
	 * Initializes ConfusionMatrix objects from the resulting evaluations 
	 */
	private void loadConfusionMatrices()throws Exception{		
		confMatrices = new ConfusionMatrix[classifierModels.length];//Initializes Confusion Matrix array
		
		for(int k = 0; k < classifierModels.length; k++){	//loops through confusion matrix array
			double[][] doubleMatrix = eval[k].confusionMatrix();
			confMatrices[k] = new ConfusionMatrix(classNominalVal.size(),docClasses.split("\n"));
			for(int j = 0; j < numClasses; j++){
				confMatrices[k].setRow(j,ArrayUtils.toObject(doubleMatrix[j]));
			}
		}	
	}

}

