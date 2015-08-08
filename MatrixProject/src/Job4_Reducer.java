import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Vector;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configuration;

import weka.classifiers.Evaluation;
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

import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import weka.classifiers.AggregateableEvaluation;
import weka.classifiers.Evaluation;


public class Job4_Reducer extends Reducer <Text,Text,Text,Text> {

	static String[] classifierNames = null;
	ArrayList<String>[] resultModelsMatrix = null;
	double[] avgModelsF1Scores = null;
	static String outputPath = null, docClasses = null;
	static int reducerNum = 0, totalFeatures = 0, numClasses = 0, numFolds = 0, keyCount = 0;

	static Instances dataset = null, tempSet = null;

	FastVector<Attribute> fvWekaAttributes = new FastVector<Attribute>();
	FastVector<String> classNominalVal = null;
	HashMap<String,Integer> classifierIndexMap = null;
	MultipleOutputs<Text, Text> mos;




	public void setup(Context context) {
		TaskAttemptID tid = context.getTaskAttemptID();		
		String[] splitter = tid.toString().split("_");
		reducerNum = Integer.parseInt(splitter[4]);

		System.out.println("\n****************** Processing Job 4 Reducer: "+reducerNum+ " ******************\n");

		Configuration conf = context.getConfiguration();
		outputPath = conf.get("modelsPath");
		docClasses = conf.get("docClasses");
		numClasses = Integer.parseInt(conf.get("numClasses"));
		totalFeatures = Integer.parseInt(conf.get("totalFeatures"));
		numFolds = Integer.parseInt(conf.get("numFolds"));
		classifierNames = conf.get("parClassifiers").split("\t");

		resultModelsMatrix = new ArrayList<String>[numFolds]();
		avgModelsF1Scores = new double[classifierNames.length];

		classifierIndexMap = new HashMap<String,Integer>(classifierNames.length);


	}

	public void reduce(Text key, Iterable<Text> values, Context context)throws IOException , InterruptedException{

		//	System.out.println(key.toString());
		//replaces the classifier name string values with the order in which they come from the reducer for later use
		//	classifierNames[keyCount] = key.toString();
		if(!classifierIndexMap.containsKey(key.toString()))
			classifierIndexMap.put(key.toString(),keyCount);

		String[] splitter = null;

		double FScoreAvg = 0;
		int numClassifiers = 0;
		int tempCount = 0;
		for(Text value: values){
	
			splitter = value.toString().split("\n",2);
			if(classifierIndexMap.containsKey(key.toString())){
				avgModelsF1Scores[classifierIndexMap.get(key.toString())] += Double.parseDouble(splitter[0]);
				resultModelsMatrix[classifierIndexMap.get(key.toString())][tempCount] = splitter[1];
			}
			else{
				avgModelsF1Scores[keyCount] += Double.parseDouble(splitter[0]);
				resultModelsMatrix[keyCount][tempCount] = splitter[1];
				keyCount++;	

			}
			tempCount++;
		}
	//	System.out.println("Key |" + key.toString()+"| has " + tempCount + " iterables");
		avgModelsF1Scores[keyCount] = FScoreAvg / classifierNames.length;

	}

	protected void cleanup(Context context) throws IOException, InterruptedException {

		mos = new MultipleOutputs<Text,Text>(context);

		try{
		//	for(int k = 0; k < classife)
		//	System.out.println("Key "+key.toString() +"Value "+ value.toString());
			int bestClassifierIndex = getBestClassifier();
			String xmlMatrices = "";
			//puts all the matrices of the best classifer into a string to output 
			for(int k = 0; k < numFolds; k++)
				xmlMatrices += resultModelsMatrix[bestClassifierIndex][k];

			context.write(new Text(classifierNames[bestClassifierIndex]+"\n\n"), new Text(xmlMatrices));

			//mos.write("ReducerResult"+reducerNum, new Text(classifierModels[k].getClass().getSimpleName()+"\n\tF0.5"+"\tF1"+"\tF2\n" +confMatrices[k].getFMeasures()),new Text(""));
			//new Text("Model "+ classifierModels[k].getClass().getSimpleName() + "\n"+eval[k].toMatrixString(classifierModels[k].getClass().getSimpleName())),new Text(""));

		}catch(Exception e){
			e.printStackTrace();
		}
		mos.close();
	}

	private int getBestClassifier(){

		int maxScoreIndex = 0;
		double maxFScore = 0;

		for(int k = 0; k < avgModelsF1Scores.length; k++){
			if(avgModelsF1Scores[k] > maxFScore)
				maxScoreIndex = k;		
		}
		return maxScoreIndex;
	}

}