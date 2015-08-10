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

		System.out.println("\n****************** Processing Job 5 Reducer: "+reducerNum+ " ******************\n");

		Configuration conf = context.getConfiguration();
		outputPath = conf.get("modelsPath");
		docClasses = conf.get("docClasses");
		numClasses = Integer.parseInt(conf.get("numClasses"));
		totalFeatures = Integer.parseInt(conf.get("totalFeatures"));
		numFolds = Integer.parseInt(conf.get("numFolds"));
		classifierNames = conf.get("parClassifiers").split("\t");
		classifierOptionsArray = conf.get("parClassifiers").split("\t");

		resultModelsMatrix = new ArrayList[classifierNames.length];
		for(int k =0; k< classifierNames.length;k++)
			resultModelsMatrix[k] = new ArrayList<String>();

		avgModelsF1Scores = new double[classifierNames.length];

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
			splitter2 = splitter[1].split("\t");
			
		//	System.out.println("FScore " + splitter2[1]);
			
			if(classifierIndexMap.containsKey(key.toString())){
				avgModelsF1Scores[classifierIndexMap.get(key.toString())] += Double.parseDouble(splitter2[1]);
				resultModelsMatrix[classifierIndexMap.get(key.toString())].add(value.toString());
			}
			else{
				classifierIndexMap.put(key.toString(), keyCount);
				avgModelsF1Scores[keyCount] += Double.parseDouble(splitter2[1]);
				resultModelsMatrix[keyCount].add(value.toString());
				//arranges the values of the class name array so that it matches the avg score one 
				String[] tempClassifierOptions = classifierOptionsArray[keyCount].split(",");
				if(tempClassifierOptions.length == 2){
			//		System.out.println("Putting classifier "+key.toString()+" in index " + keyCount);
					classifierNames[keyCount] = key.toString() + "| With options "+ tempClassifierOptions[1];
				}
				else{
			//		System.out.println("Putting classifier "+key.toString()+" in index " + keyCount);
					classifierNames[keyCount] = key.toString() + "| ";
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
			int bestClassifierIndex = getBestClassifierIndex();
			System.out.println("The Best Index is "+bestClassifierIndex);
			//sortMatricesArray();
			String xmlMatrices = "";
			//puts all the matrices of the best classifer into a string to output 
			for(int k = 0; k < numFolds; k++)
				xmlMatrices += resultModelsMatrix[bestClassifierIndex].get(k);

			context.write(new Text(classifierNames[bestClassifierIndex]+"\tAvg F score:"+avgModelsF1Scores[bestClassifierIndex]/numFolds
					+"\n\n"), new Text(xmlMatrices));

			//mos.write("ReducerResult"+reducerNum, new Text(classifierModels[k].getClass().getSimpleName()+"\n\tF0.5"+"\tF1"+"\tF2\n" +confMatrices[k].getFMeasures()),new Text(""));
			//new Text("Model "+ classifierModels[k].getClass().getSimpleName() + "\n"+eval[k].toMatrixString(classifierModels[k].getClass().getSimpleName())),new Text(""));

		}catch(Exception e){
			e.printStackTrace();
		}
		mos.close();
	}

	private int getBestClassifierIndex(){

		int maxScoreIndex = 0;
		double maxFScore = 0;

		for(int k = 0; k < avgModelsF1Scores.length; k++){
			System.out.println("Avg F score for classifier "+ classifierNames[k] +" "+avgModelsF1Scores[k]/10);
			if(avgModelsF1Scores[k] > maxFScore){
				maxFScore = avgModelsF1Scores[k];
				maxScoreIndex = k;		
			}
		}
		return maxScoreIndex;
	}

	private void sortMatricesArray(){
		int bestClassifierIndex = getBestClassifierIndex();
		String[] tempArrayToSort = new String[numFolds];
		for(int k = 0; k < tempArrayToSort.length; k++){
			tempArrayToSort[k] = (String)resultModelsMatrix[bestClassifierIndex].get(k);
		}
		
		Arrays.sort(tempArrayToSort, new Comparator<String>() {
					@Override
					public int compare(String par1, String par2) {
						int index1 = 0, index2 = 0;
						String[] splitter = par1.split("\n",2);
						splitter = splitter[0].split(" ");
						index1 = Integer.parseInt(splitter[3]);
						
						splitter = par2.split("\n",2);
						splitter = splitter[0].split(" ");
						index2 = Integer.parseInt(splitter[3]);
						return Integer.valueOf(index1).compareTo(Integer.valueOf(index2));
					}
				});
		resultModelsMatrix[bestClassifierIndex].clear();
		
		for(int k = 0; k < tempArrayToSort.length;k++)
			resultModelsMatrix[bestClassifierIndex].add((Object)tempArrayToSort[k]);
			
	}

}