import java.io.File;
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
import weka.classifiers.trees.DecisionStump;
import weka.classifiers.trees.J48;
import weka.classifiers.rules.*;
import weka.classifiers.bayes.*;
import weka.classifiers.lazy.*;
import weka.classifiers.evaluation.ConfusionMatrix;
import weka.core.SparseInstance;
import weka.core.Attribute;

import java.nio.file.FileSystem;

import weka.classifiers.trees.RandomTree; 
import weka.classifiers.rules.ZeroR ;
import weka.classifiers.trees.REPTree;
import weka.core.FastVector;
public class Job4_Combiner extends Reducer<IntWritable,Text,IntWritable,Text> {

	static Instances dataset = null;
	static int totalFeatures = 0;
	static ArrayList<Attribute> attributes = new ArrayList<Attribute>();
	static Classifier[] models = { new J48(),new PART(),new DecisionTable(),new DecisionStump()
	,new J48(), new BayesNet(),new KStar(),new ZeroR(),new OneR(),new REPTree()};
//		new J48(),new PART(), new DecisionTable(),new DecisionStump(), //one-level decision tree
//		new NaiveBayes(), new BayesNet()
//		,new KStar(), new J48(), new LWL(), new ZeroR()		
//	};
	AggregateableEvaluation evals[] = new AggregateableEvaluation[models.length];
	AggregateableEvaluation tempEval = null;
	FastVector<Attribute> fvWekaAttributes = new FastVector<Attribute>();
	 
	public void setup(Context context){
		System.out.println("\n******** Processing Job 4 Combiner ********\n");
		Configuration conf = context.getConfiguration();
		totalFeatures = Integer.parseInt(conf.get("totalFeatures"));
		/*
		 * Creates Array of attributes to make into the instance data
		 */
		String temp = null;
		for(int k =0; k < totalFeatures; k++){
			temp = k + " Feature";
			fvWekaAttributes.addElement(new Attribute("feature "+ k));
		//	attributes.add(new Attribute(temp,k));
			}

		List<String> classifierList = new ArrayList<String>();
		classifierList.add("Rec.Autos");
		classifierList.add("talk.politics.mideast");
		//attributes.add(new Attribute("Classifiers",classifierList));	
		FastVector fvNominalVal = new FastVector(2);
		 fvNominalVal.addElement("Rec.Autos");
		 fvNominalVal.addElement("talk.politics.mideast");
		// fvWekaAttributes.addElement( new Attribute("Document Class", fvNominalVal));
		 fvWekaAttributes.addElement( new Attribute("Document Class", fvNominalVal));

		try{
			dataset = new Instances("FeatureInstance",fvWekaAttributes,totalFeatures+1); 		
			dataset.setClassIndex(fvWekaAttributes.size()-1);
			System.out.println(dataset.classAttribute().toString());
			//instantiates evaluation objects
			for(int k=0; k < evals.length; k++)
				evals[k] = new AggregateableEvaluation(dataset);
			
		}catch(Exception e){
			e.printStackTrace();
		}
	}


	public void reduce(IntWritable key, Iterable<Text> values,  Context context) throws IOException, InterruptedException {

		String[] lines = null, splitLine = null; 
		String instanceClass = null;
		double[] InstanceValues = null;
		int[] InstanceIndices = null;

		dataset.setClassIndex(fvWekaAttributes.size()-1);
		//System.out.println(dataset.classIndex());

		//if(key.get() == 0){
		for (Text val : values) {

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
		
			//System.out.println(Arrays.toString(lines));
			
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

			dataset.add(instanceRow);	
			context.write(new IntWritable(key.get()), new Text(val.toString()));

		}
		/*
		 * End of Iterable For loop 
		 */
		try{
		//	System.out.println("Buidling Classifier: " + key.get() + " on "+dataset.numInstances()+" instances.");
			models[key.get()].buildClassifier(dataset);
			//System.out.println(dataset.toSummaryString());

			dataset.delete();

	/*		for(int k = 0; k < key.get(); k++){

				if(k == key.get()-1){
					//If the dataset is evaluated from the previous trained model,
					//then it insantiates the new Evalutation object instead of aggregating 
					evals[k].evaluateModel(models[k],dataset);
		//			System.out.println("Classifier # " +k+ " Evaluating training Fold: "+ key.get()+ "\tFirst Instantiation");
				}
				else{
		//			System.out.println("Classifier # " +k+ " Evaluating training Fold: "+ key.get());
					tempEval = new AggregateableEvaluation(dataset);
					tempEval.evaluateModel(models[k],dataset);
					evals[key.get()].aggregate(tempEval);
				}
			}
			*/


		}catch (Exception e){
			e.printStackTrace();
		}
		//	}
		//System.out.println("Combiner Key: "+key.get());

	}


	protected void cleanup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		String outputPath = conf.get("modelsPath");
		File ClassifierModelsDir = new File(outputPath+"Models/");
		ClassifierModelsDir.mkdirs();
		File EvaluationModelsDir = new File(outputPath+"Evaluations/");
		EvaluationModelsDir.mkdirs();
		
		System.out.println("\nJob 4 Clean up | Writing out Classifier and Evaluations\n");
		try{
			for(int k = 0; k < models.length; k++){
			//	System.out.println("Classifier: "+k+" ACCURACY: "+evals[k].pctCorrect()+"%");
				weka.core.SerializationHelper.write(outputPath+"Models/"+k+".model", models[k]);
			//	weka.core.SerializationHelper.write(outputPath+"Evaluations/"+k+".evaluation", evals[k]);

			}
			System.out.println("\nWrote out Classifiers and Evaluations sucessfully\n");
		}catch(Exception e){
			e.printStackTrace();
		}
	}

}



