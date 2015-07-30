import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.io.IntWritable;

import weka.classifiers.AggregateableEvaluation;
import weka.classifiers.bayes.NaiveBayes;
import weka.classifiers.functions.Logistic;
import weka.core.Attribute;
import weka.core.Instances;
import weka.core.SparseInstance;
import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.evaluation.NominalPrediction;
import weka.classifiers.rules.*;
import weka.classifiers.trees.*;
import weka.classifiers.bayes.*;
import weka.classifiers.lazy.*;
import weka.classifiers.evaluation.ConfusionMatrix;
import weka.core.FastVector;
import weka.core.Instances;
import weka.core.converters.ArffSaver;
import weka.core.converters.ArffLoader;
import weka.filters.Filter;
import weka.filters.supervised.attribute.AddClassification;
import weka.core.Attribute;
import java.util.*;




public class Job_4Temp_Mapper extends Mapper<Text, Text, IntWritable, Text>{

	public enum Job_4Temp_Mapper_Counter { LINES }
	public static long totalDocuments = 0;
	public int testNum = 0, instanceSize = 0;
	public static int[] documentPartitions = new int[10];

	static int totalFeatures = 0;
	static ArrayList<Attribute> attributes = new ArrayList<Attribute>();

	static Classifier[] models = { 
		new J48(),new PART(), new DecisionTable(),new DecisionStump(), //one-level decision tree
		new NaiveBayes(), new BayesNet()
		,new KStar(), new IBk(), new LWL()		
	};

	static Instances dataset = null;

	Random randomGenerator = new Random();

	public void setup(Context context) {
		System.out.println("\n******** Processing Job 4 Mapper ********\n");

		Configuration conf = context.getConfiguration();
		totalDocuments = Long.parseLong(conf.get("totalDocuments"));
		totalFeatures = Integer.parseInt(conf.get("totalFeatures"));
		instanceSize = Integer.parseInt(conf.get("instanceSize"));

		for(int k = 0; k < documentPartitions.length; k++){
			documentPartitions[k] = (int)((k+1) * totalDocuments/10);
			//	System.out.println(documentPartitions[k]);
		}

		for(int k =0; k < totalFeatures + 1 ; k++)
			attributes.add(new Attribute(""+k));

		ArrayList<String> classifierList = new ArrayList<String>();
		classifierList.add("Rec.Autos");
		classifierList.add("talk.politics.mideast");
		attributes.add(new Attribute("Classifiers",classifierList));
		/*
		 * Instantiates the Dataset and the Classifers
		 */
		try{
			dataset = new Instances("FeatureInstance",attributes,totalFeatures+1);
			dataset.setClassIndex(totalFeatures+1);



		}catch(Exception e){
			e.printStackTrace();
		}

		TaskAttemptID tid = context.getTaskAttemptID();
		System.out.println(tid.toString());


	}
	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
	 * 
	 * 
	 */
	public void map(Text docID_Classifier_Text, Text feature_Set, Context context) throws IOException, InterruptedException {
		context.getCounter(Job_4Temp_Mapper_Counter.LINES).increment(1);//increments the counter so it can be used as indexer
		long docCounter = context.getCounter(Job_4Temp_Mapper_Counter.LINES).getValue();

		if(docCounter%instanceSize != 0 ){
			//	System.out.println(docCounter);

			String[] DocID_Classifier_Str = docID_Classifier_Text.toString().split("\t");
			String[] lines = feature_Set.toString().split("\n");

			/*
			 * Sorts the features by their index so thta they can be added in order to the spareInstance object. Ordered sparse instances are required by weka 
			 
			
			if(!feature_Set.toString().equals("")){	
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
			}

*/

			//System.out.println(feature_Set.toString());
			String[] splitLine = null;

			double[]InstanceValues = new double[lines.length];
			int[] InstanceIndices = new int[lines.length];
			/*
			if(!feature_Set.toString().equals("")){		//checks for empty featureList. If empty then the 0's are passed to the instance 
				for(int k = 0; k < lines.length; k++){
					splitLine = lines[k].split("\t");
					InstanceIndices[k] = Integer.parseInt(splitLine[0]);
					InstanceValues[k] = Double.parseDouble(splitLine[1]);
				}
			}
			 */

			/*
			 * Builds Instance Row From the Value in the Loop
			 */
			SparseInstance instanceRow = new SparseInstance(1.0,InstanceValues,InstanceIndices,totalFeatures + 1);
			if(DocID_Classifier_Str[0].equals("rec.autos"))
				instanceRow.setValue(totalFeatures+1, 0.5);
			else	
				instanceRow.setValue(totalFeatures+1, 1.0);

			dataset.add(instanceRow);	
			//	System.out.println(instanceRow.toString());

		}
		if(docCounter%instanceSize == 0){
			try{
				//	System.out.println(dataset.toString());
				testNum++;
				System.out.println("********** Test Run # "+testNum+" on "+dataset.numInstances()+" Instances ***********\n");
		//		runCrossValidation(models,dataset,10);
				System.out.println("**********            ***********\n");
				dataset.delete();

			}catch(Exception e){
				e.printStackTrace();
			}
		}
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		try{
			if(dataset.numInstances() != 0){
				testNum++;
				System.out.println("\n********** Left over number of instances: "+dataset.numInstances()+"  ***********\n");
				System.out.println("********** Test Run # "+testNum+" on "+dataset.numInstances()+" Instances ***********\n");
		//		runCrossValidation(models,dataset,10);
				System.out.println("**********            ***********\n");
				dataset.delete();
			}

		}catch(Exception e){
			e.printStackTrace();
		}

	}
}

/*

	public static void runCrossValidation (Classifier[] models, Instances data, int numOfFolds) throws Exception{

		Instances[][] split = crossValidationSplit(data, numOfFolds);
		Instances[] trainingSplits = split[0];
		Instances[] testingSplits = split[1];

		System.out.println("Classifying using " + numOfFolds+ "-cross validation" );

		for (int j = 0; j < models.length; j++) {

			// Collect every group of predictions for current model in a FastVector
			FastVector predictions = new FastVector();

			// For each training-testing split pair, train and test the classifier
			for (int i = 0; i < trainingSplits.length; i++) {
				Evaluation validation = classify(models[j], trainingSplits[i], testingSplits[i]);

				predictions.appendElements(validation.predictions());

				// Uncomment to see the summary for each training-testing pair.
				//System.out.println(models[j].toString());
			}

			// Calculate overall accuracy of current classifier on all splits
			double accuracy = calculateAccuracy(predictions);

			// Print current classifier's name and accuracy in a complicated,
			// but nice-looking way.
			System.out.println("Accuracy of " + models[j].getClass().getSimpleName() + ": "
					+ String.format("%.2f%%", accuracy)
					+ "\n---------------------------------");
		}
	}

	public static Instances[][] crossValidationSplit(Instances data, int numberOfFolds) {
		Instances[][] split = new Instances[2][numberOfFolds];

		for (int i = 0; i < numberOfFolds; i++) {
			split[0][i] = data.trainCV(numberOfFolds, i);
			split[1][i] = data.testCV(numberOfFolds, i);
		}
		return split;
	}
	public static Evaluation classify(Classifier model,Instances trainingSet, Instances testingSet) throws Exception {
		Evaluation evaluation = new Evaluation(trainingSet);

		model.buildClassifier(trainingSet);
		evaluation.evaluateModel(model, testingSet);

		return evaluation;
	}

	public static double calculateAccuracy(FastVec		context.write(new IntWritable(randomGenerator.nextInt(10)), new Text(docID_Classifier_Str[0] + "\t"+ docID_Classifier_Str[1]+"\n"+feature_Set.toString())); 
tor predictions) {
		double correct = 0;

		for (int i = 0; i < predictions.size(); i++) {
			NominalPrediction np = (NominalPrediction) predictions.elementAt(i);
			if (np.predicted() == np.actual()) {
				correct++;
			}
		}

		return 100 * correct / predictions.size();
	}

}

*/

/*
 * 
 * 
 * 		String[] docID_Classifier_Str = docID_Classifier_Text.toString().split("\t");

	//	System.out.println(docID_Classifier_Str[0] + "\t"+ docID_Classifier_Str[1]);


		for(int k = 0; k < documentPartitions.length; k++){		
			if(docCounter <= documentPartitions[k]){
				//System.out.println("Document#: " + docCounter+ " Partition Key: " + k + " " +DocID.toString());
				context.write(new IntWritable(k), new Text(DocID + line.toString()));
				break;
			}
		}



//		System.out.println("Document#: " + docCounter+ " " + randomGenerator.nextInt((int)totalDocuments/10));
		context.write(new IntWritable(randomGenerator.nextInt(10)), new Text(docID_Classifier_Str[0] + "\t"+ docID_Classifier_Str[1]+"\n"+feature_Set.toString())); 
 * 
 * 
 * 
 * 
 * 
 * 
 * 
 * 
 * 
 * 
 * 
 */