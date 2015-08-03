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
import weka.core.converters.ArffSaver;
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

	static Instances dataset = null, tempSet = null;
	static int totalFeatures = 0;
	static FastVector<Attribute> fvWekaAttributes = new FastVector<Attribute>();
	String outputPath = null;
	static Classifier[] models = { new J48(),new PART(),new DecisionTable(),new DecisionStump()
	,new NaiveBayes(), new BayesNet(),new KStar(),new ZeroR(),new OneR(),new REPTree()};

	AggregateableEvaluation tempEval = null;

	public void setup(Context context){
		System.out.println("\n******** Processing Job 4 Combiner ********\n");

		Configuration conf = context.getConfiguration();
		totalFeatures = Integer.parseInt(conf.get("totalFeatures"));
		outputPath = conf.get("modelsPath");
		
		File ClassifierModelsDir = new File(outputPath+"Models/");
		ClassifierModelsDir.mkdirs();
		/*
		 * Creates Array of attributes to make into the instance data
		 */
		for(int k =0; k < totalFeatures; k++)
			fvWekaAttributes.addElement(new Attribute("Feature "+ k));


		FastVector fvNominalVal = new FastVector(2);
		fvNominalVal.addElement("Rec.Autos");
		fvNominalVal.addElement("talk.politics.mideast");

		fvWekaAttributes.addElement( new Attribute("Class Attribute", fvNominalVal));

		System.out.println("Instance Vector Size: " + fvWekaAttributes.size());

		try{
			dataset = new Instances("FeatureInstance",fvWekaAttributes,fvWekaAttributes.size()-1); 	
			tempSet = new Instances(dataset);

			dataset.setClassIndex(fvWekaAttributes.size()-1);
			System.out.println(dataset.classAttribute().toString());
			//instantiates evaluation objects
			//	System.out.println(dataset.toSummaryString());

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

			dataset.add(instanceRow);	
		//	tempSet.add(instanceRow);

			context.write(new IntWritable(key.get()), new Text(val.toString()));

		}
		/*
		 * End of Iterable For loop 
		 */
		try{
		//	System.out.println("Buidling Classifier: " + key.get() + " on "+dataset.numInstances()+" instances.");
			models[key.get()].buildClassifier(dataset);
			//System.out.println("Classifier: " + key.get()+ " " +models[key.get()].toSummaryString());
			weka.core.SerializationHelper.write(outputPath+"Models/"+key.get()+".model", models[key.get()]);
			weka.core.SerializationHelper.write("/home/cloudera/Desktop/tempModels/"+key.get()+".model", models[key.get()]);
			//System.out.println(dataset.toString());
			dataset.delete();



		}catch (Exception e){
			e.printStackTrace();
		}

	}


	protected void cleanup(Context context) throws IOException, InterruptedException {

	//	System.out.println(tempSet.toString());


		
		File EvaluationModelsDir = new File(outputPath+"Evaluations/");
		EvaluationModelsDir.mkdirs();

	
			//System.out.println(tempDataset.toString());


	}

}



