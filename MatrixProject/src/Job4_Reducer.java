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


public class Job4_Reducer extends Reducer <IntWritable,Text,Text,Text> {

	static String[] classifiersToEvalNames = null;
	static String outputPath = null, docClasses = null;
	static Classifier[] classifierModels = null;
	static int reducerNum = 0, totalFeatures = 0, numClasses = 0;
	AggregateableEvaluation[] eval = null;
	Evaluation tempEval = null;

	static Instances dataset = null, tempSet = null;

	FastVector<Attribute> fvWekaAttributes = new FastVector<Attribute>();
	FastVector<String> classNominalVal = null;
	HashMap<String,Double> classNominalMap = null;
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

		classifiersToEvalNames = conf.get("parClassifiers").split("\t");
		loadClassifiers(classifiersToEvalNames);
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
			classNominalMap.put(splitter[k].trim(), 1.0 / (k+1.0));
			System.out.println(classNominalMap.get(splitter[k].trim()));
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

	public void reduce(IntWritable key, Iterable<Text> values, Context context)throws IOException , InterruptedException{

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

			instanceRow.setValue(fvWekaAttributes.size()-1, classNominalMap.get(instanceClass));
			/*
			if(instanceClass.equals("rec.autos"))
				instanceRow.setValue(fvWekaAttributes.size()-1, 0.5);
			else	
				instanceRow.setValue(fvWekaAttributes.size()-1, 1.0);
			 */
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

			//	context.write(new IntWritable(key.get()), new Text(val.toString()));

		}

		//	if(reducerNum == 9)
		//	System.out.println(dataset.toString());
		//	System.out.println(tempCount);

	}



	protected void cleanup(Context context) throws IOException, InterruptedException {

		mos = new MultipleOutputs<Text,Text>(context);

		ConfusionMatrix[] confMatrices = new ConfusionMatrix[classifierModels.length];
		try{
			System.out.println("\nEvaluation for Model: " + classifierModels[0].getClass().getSimpleName() + " | "+ eval[0].pctCorrect());

			/*
			for(int k = 0; k < classifierModels.length; k++){
				double[][] doubleMatrix = eval[k].confusionMatrix();
				confMatrices[k] = new ConfusionMatrix(classNominalVal.size(),docClasses.split("\n"));
				for(int j = 0; j < classNominalVal.size(); j++){
					confMatrices[k].setRow(j,ArrayUtils.toObject( doubleMatrix[j]));
				}
			 */
			for(int k = 0; k < classifierModels.length; k++){

				mos.write("ReducerModel"+k,new Text("Model "+ classifierModels[k].getClass().getSimpleName() + "\n"+
						eval[k].toMatrixString(classifierModels[k].getClass().getSimpleName())),new Text(""));
			}
			//matrix.display();
			//new Text(eval.toMatrixString(reducerClassifier.getClass().getSimpleName()+ " Confusion Matrix")));
		}catch(Exception e){
			e.printStackTrace();
		}
		mos.close();
	}

	/*
	 * Parameter: String[] of the names of the classifiers to be evaluated
	 * Each String contains the classifier name and their options so they need 
	 * split in order to just use the classifier name to load the classifiers 
	 * externally.
	 */
	private void loadClassifiers(String[] classifiersPar){
		
		String[] splitter = null;
		System.out.println("\nReading in model for Testing....\n" );
		try{
			classifierModels = new Classifier[classifiersPar.length];
			for(int k = 0; k < classifiersPar.length;k++){
				splitter = classifiersPar[k].split(",");
				classifierModels[k] = (Classifier) weka.core.SerializationHelper.read(outputPath+"Models/Mapper_"+reducerNum +"/"+splitter[0].trim()+".model");
			}

		}catch(Exception e){
			e.printStackTrace();
		}
		System.out.println("\nRead in models succesfully!\n");
	}

}