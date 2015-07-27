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
import weka.core.SparseInstance;
import weka.core.Attribute;

public class Job4_Combiner extends Reducer<IntWritable,Text,IntWritable,Text> {

	static int totalFeatures = 0;
	static ArrayList<Attribute> attributes = new ArrayList<Attribute>();

	public void setup(Context context){
		System.out.println("\n******** Processing Job 4 Combiner ********\n");
		Configuration conf = context.getConfiguration();
		totalFeatures = Integer.parseInt(conf.get("totalFeatures"));


		for(int k =0; k < totalFeatures + 1 ; k++){
			attributes.add(new Attribute(""+k));
			//class attribute 
		}
		ArrayList classifierList = new ArrayList();
		classifierList.add("Rec.Autos");
		classifierList.add("talk.politics.mideast");
		attributes.add(new Attribute("Classifiers",classifierList));

	}


	public void reduce(IntWritable key, Iterable<Text> values,  Context context) throws IOException, InterruptedException {

		String[] lines = null, splitLine = null; 
		String DocID = null, classifier = null;

		double[] InstanceValues = null;
		int[] InstanceIndices = null;

		SparseInstance sparseRow = null;

		Instances dataset = new Instances("FeatureInstance",attributes,totalFeatures+1);
		dataset.setClassIndex(totalFeatures+1);
		//System.out.println(dataset.classIndex());


		int count = 0;
		if(key.get() == 0){
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

					SparseInstance instanceRow = new SparseInstance(1.0,InstanceValues,InstanceIndices,totalFeatures + 1);
					if(classifier.equals("rec.autos"))
						instanceRow.setValue(totalFeatures+1, 0.5);
					else	
						instanceRow.setValue(totalFeatures+1, 1.0);

					dataset.add(instanceRow);
					count++;		
				
			}

		
			System.out.println(dataset.toString());




		}

	}
}
