import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configuration;

import weka.core.Instance;
import weka.core.InstanceComparator;
import weka.core.Instances;
import weka.core.converters.ArffLoader.ArffReader;
import weka.core.converters.ConverterUtils.DataSource;
import weka.core.Utils;
import weka.classifiers.AbstractClassifier;
import weka.classifiers.AggregateableEvaluation;
import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.trees.j48.*;
import weka.classifiers.rules.*;
import weka.classifiers.bayes.*;
import weka.classifiers.lazy.*;
import weka.classifiers.evaluation.ConfusionMatrix;
import weka.core.SparseInstance;
import weka.core.Attribute;
import weka.classifiers.functions.Logistic;
import weka.classifiers.functions.SGD;


public class Job4_Combiner extends Reducer<IntWritable,Text,IntWritable,Text> {

	static int totalFeatures = 0;
	static ArrayList<Attribute> attributes = new ArrayList<Attribute>();
	static AggregateableEvaluation[] NB_Eval = new AggregateableEvaluation[2];
	static AggregateableEvaluation[] LG_Eval = new AggregateableEvaluation[2];
	static AggregateableEvaluation[] SGD_Eval = new AggregateableEvaluation[2];


	static String classname = "weka.classifiers.trees.J48";
	static String[] opts = {"","-C",".25"};
	
	static NaiveBayes[] NB_Classifier = {new NaiveBayes(),new NaiveBayes()};
	static Logistic[] LG_Classifier = {new Logistic(), new Logistic()};
	static SGD[] SGD_Classifier = {new SGD(), new SGD()};

	
	static Integer testInt = null;
	static Instances dataset = null;

	static int count = 0;



	public void setup(Context context){
		System.out.println("\n******** Processing Job 4 Combiner ********\n");
		Configuration conf = context.getConfiguration();
		totalFeatures = Integer.parseInt(conf.get("totalFeatures"));
		/*
		 * Creates Array of attributes to make into the instance data
		 */
		for(int k =0; k < totalFeatures + 1 ; k++)
			attributes.add(new Attribute(""+k));

		ArrayList classifierList = new ArrayList();
		classifierList.add("Rec.Autos");
		classifierList.add("talk.politics.mideast");
		attributes.add(new Attribute("Classifiers",classifierList));
		/*
		 * Instantiates the Dataset and the Classifers
		 */
		try{
			dataset = new Instances("FeatureInstance",attributes,totalFeatures+1);
			dataset.setClassIndex(totalFeatures+1);

			//	cls = (Classifier) new NaiveBayes();//Utils.forName(Classifier.class, classname, opts);
			//	clsCopy = (Classifier) new NaiveBayes();//Utils.forName(Classifier.class, classname, opts);
			NB_Eval[0] = new AggregateableEvaluation(dataset);
			NB_Eval[1] = new AggregateableEvaluation(dataset);
			LG_Eval[0] = new AggregateableEvaluation(dataset);
			LG_Eval[1] = new AggregateableEvaluation(dataset);
			SGD_Eval[0] = new AggregateableEvaluation(dataset);
			SGD_Eval[1] = new AggregateableEvaluation(dataset);


		}catch(Exception e){
			e.printStackTrace();
		}

	}


	public void reduce(IntWritable key, Iterable<Text> values,  Context context) throws IOException, InterruptedException {

		String[] lines = null, splitLine = null; 
		String DocID = null, classifier = null;

		double[] InstanceValues = null;
		int[] InstanceIndices = null;

		SparseInstance sparseRow = null;

		dataset.setClassIndex(totalFeatures+1);
		//System.out.println(dataset.classIndex());


		if(key.get() <= 6){
			for (Text val : values) {

				lines = val.toString().split("\n");
				splitLine = lines[0].split("\t");
				DocID = splitLine[0];
				classifier = splitLine[1];

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
				if(classifier.equals("rec.autos"))
					instanceRow.setValue(totalFeatures+1, 0.5);
				else	
					instanceRow.setValue(totalFeatures+1, 1.0);

				dataset.add(instanceRow);

				/*
				 * Trains the Classifier if its the the First Key
				 * Tests it if its not the first one 
				 */

				try {
					/*
					 * Trains using the dataset from the first key
					 */

					if((key.get() <= 5)){
						if(key.get() == 0 && count == 0){
							NB_Classifier[0].buildClassifier(dataset);
							LG_Classifier[0].buildClassifier(dataset);
							SGD_Classifier[0].buildClassifier(dataset);

							dataset.delete();
							count++;		
						}
						else{
							NB_Classifier[1].buildClassifier(dataset);
							LG_Classifier[1].buildClassifier(dataset);
							SGD_Classifier[1].buildClassifier(dataset);

							
							NB_Classifier[0] = NB_Classifier[0].aggregate(NB_Classifier[1]);
							LG_Classifier[0] = LG_Classifier[0].aggregate(LG_Classifier[1]);
							
							SGD_Classifier[0] = SGD_Classifier[0].aggregate(SGD_Classifier[1]);

							dataset.delete();
							count++;		
						}
					}
					if( (key.get() == 6)   ){
						//System.out.println("Evaluating the model...");
						NB_Eval[1].evaluateModel(NB_Classifier[0], dataset);
						NB_Eval[0].aggregate(NB_Eval[1]);
						
						LG_Eval[1].evaluateModel(LG_Classifier[0], dataset);
						LG_Eval[0].aggregate(LG_Eval[1]);
						
						SGD_Eval[1].evaluateModel(LG_Classifier[0], dataset);
						SGD_Eval[0].aggregate(SGD_Eval[1]);
	
						dataset.delete();

						//System.out.println(dataset.toString());
					}


					//System.out.println(dataset.toString());
					//    clsCopy = AbstractClassifier.makeCopy(cls);
					//    System.out.println("Training the classifier...");




				}catch(Exception e){
					e.printStackTrace();
				}
			}
			/*
			 * End of Iterable loop
			 */

			//	System.out.println(cls.toString());


			



		}
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		System.out.println("Number of Insances trained "+ count);
		System.out.println("Naive Bayes Acurracy Ratio: "+ NB_Eval[0].pctCorrect()+" | " +NB_Eval[0].pctIncorrect() );
		System.out.println("Logistic Acurracy Ratio: "+ LG_Eval[0].pctCorrect()+" | " +LG_Eval[0].pctIncorrect() );
		System.out.println("Random Forest Acurracy Ratio:  "+ SGD_Eval[0].pctCorrect()+" | " +SGD_Eval[0].pctIncorrect());
	}
}



