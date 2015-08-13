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


public class Job5_Reducer extends Reducer <Text,Text,Text,Text> {

	static String[] classifierNames = null, classifierOptionsArray =null;
	ArrayList[] resultModelsMatrix = null;
	String[] modelsClassFScores = null, docClasses = null;
	static String outputPath = null;
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

		System.out.println("\n****************** Processing Job 5 Reducer: "+reducerNum+ " ******************\n");

		Configuration conf = context.getConfiguration();
		outputPath = conf.get("modelsPath");
		docClasses = conf.get("docClasses").split("\n");
		numClasses = Integer.parseInt(conf.get("numClasses"));
		totalFeatures = Integer.parseInt(conf.get("totalFeatures"));
		numFolds = Integer.parseInt(conf.get("numFolds"));
		classifierNames = conf.get("parClassifiers").split("\t");
		classifierOptionsArray = conf.get("parClassifiers").split("\t");

		modelsClassFScores = new String[classifierNames.length];
		resultModelsMatrix = new ArrayList[classifierNames.length];
		//insantiates both arrays for use 
		for(int k =0; k< classifierNames.length;k++){
			resultModelsMatrix[k] = new ArrayList<String>();
			modelsClassFScores[k] = "";
		}


		classifierIndexMap = new HashMap<String,Integer>(classifierNames.length);


	}

	public void reduce(Text key, Iterable<Text> values, Context context)throws IOException , InterruptedException{

		//	System.out.println(key.toString());
		//replaces the classifier name string values with the order in which they come from the reducer for later use
		//		if(!classifierIndexMap.containsKey(key.toString()))
		//			classifierIndexMap.put(key.toString(),keyCount);

		String[] splitter = null, splitter2 = null;

		double FScoreAvg = 0;
		int numClassifiers = 0;
		int tempCount = 0;
		for(Text value: values){

			splitter = value.toString().split("\n",3);

			//	System.out.println("FScore " + splitter2[1]);

			if(classifierIndexMap.containsKey(key.toString())){
				modelsClassFScores[classifierIndexMap.get(key.toString())] += splitter[1]+"\n";
				//System.out.println("Added Class Label Data to Classifier "+ key.toString() + " Index: " + classifierIndexMap.get(key.toString()) + " | " + splitter[1]);

				resultModelsMatrix[classifierIndexMap.get(key.toString())].add(value.toString());
			}
			else{
				classifierIndexMap.put(key.toString(), keyCount);
				modelsClassFScores[keyCount] += splitter[1]+"\n";
				//	System.out.println("Added Class Label Data to Classifier "+ key.toString() + " Index: "+keyCount + " | " + splitter[1]);

				resultModelsMatrix[keyCount].add(value.toString());
				//arranges the values of the class name array so that it matches the avg score one 
				String[] tempClassifierOptions = classifierOptionsArray[keyCount].split(",");
				if(tempClassifierOptions.length == 2){
					//			System.out.println("Putting classifier "+key.toString()+" in index " + keyCount);
					classifierNames[keyCount] = key.toString() + "| With options "+ tempClassifierOptions[1];
				}
				else{
					//			System.out.println("Putting classifier "+key.toString()+" in index " + keyCount);
					classifierNames[keyCount] = key.toString();
				} 
				keyCount++;	

			}
			tempCount++;
		}
		//System.out.println("Key |" + key.toString()+"| has " + tempCount + " iterables");
		//avgModelsF1Scores[keyCount] =  classifierNames.length;

	}

	protected void cleanup(Context context) throws IOException, InterruptedException {

		mos = new MultipleOutputs<Text,Text>(context);

		try{
			//int bestClassifierIndex = getBestClassifierIndex();
			System.out.println("Number of NumClasses is "+numClasses);
			String[] bestModelClassFScoresStr = getBestClassesInModels(numClasses);	
			String xmlMatrices = "";

			for(int k = 0; k < bestModelClassFScoresStr.length; k++){
				String[] tabSplitter = bestModelClassFScoresStr[k].split("\t");
				int classifierNameIndex = Integer.parseInt(tabSplitter[0]);
				int foldIndex = Integer.parseInt(tabSplitter[1]);
				double FScore = Double.parseDouble(tabSplitter[2]);
				System.out.println("\nBest Classifier: "+classifierNames[classifierNameIndex] +" | "+ docClasses[k] +" | F-measure: "+ FScore);
				context.write(new Text("Best Classifier: "+classifierNames[classifierNameIndex] + " | "+ docClasses[k] +" | F-measure: "+ FScore),
						new Text( "\n"+findMatrixFold(classifierNameIndex, foldIndex)));
			}

		}catch(Exception e){
			e.printStackTrace();
		}
		mos.close();
	}

	private String[] getBestClassesInModels(int numClassesPar){

		String[] bestModelClassFScoresStr = new String[numClasses];
		double[] bestModelClassFScores = new double[numClasses];
		for(int k = 0; k < bestModelClassFScores.length; k++)
			bestModelClassFScores[k] = 0;
		double curF1Score = 0;
		String[] lineSplitter = null, tabSplitter= null;

		System.out.print("Classifier\tFold\t");
		for(int k = 0; k < docClasses.length;k++)
		System.out.print(docClasses[k]+"\t");
		System.out.println("\n");
		
		for(int k = 0; k < modelsClassFScores.length; k++){	//K is the classifier Name 
			lineSplitter = modelsClassFScores[k].split("\n");
			for(int j = 0; j < lineSplitter.length; j++){//J is the Fold Number 
				tabSplitter = lineSplitter[j].split("\t");
				System.out.println(classifierNames[k] + "\tFold "+j+ "\t"+ lineSplitter[j]);
				for(int i = 0; i < tabSplitter.length; i++){//i is the class index
					curF1Score = Double.parseDouble(tabSplitter[i]);
					if(curF1Score > bestModelClassFScores[i]){
						bestModelClassFScores[i] = curF1Score;
						bestModelClassFScoresStr[i] = k +"\t"+ j + "\t"+curF1Score;
					}

				}
			}

		}
		return bestModelClassFScoresStr;
	}
	
	private int getFoldMatrixNumber(int foldArrayIndex, int foldNum){
		String temp =(String) resultModelsMatrix[foldArrayIndex].get(foldNum);
		String[] splitter = temp.split("\n");
		splitter = splitter[0].split(" ");
		return Integer.parseInt(splitter[2]);
	}

	private String findMatrixFold(int foldArrayIndex, int foldNum){
		 return (String)resultModelsMatrix[foldArrayIndex].get(foldNum);
	}
}