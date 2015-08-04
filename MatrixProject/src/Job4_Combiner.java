import java.io.File;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
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

	int combinerNum = 0;

	public void setup(Context context){
		
		TaskAttemptID tid = context.getTaskAttemptID();		
		String[] splitter = tid.toString().split("_");
		combinerNum = Integer.parseInt(splitter[4]);
		
		System.out.println("\n******** Processing Job 4 Combiner " +splitter[4]+" ********\n");

		
		if(combinerNum == 0)
			combinerNum = 1;
		
	}


	public void reduce(IntWritable key, Iterable<Text> values,  Context context) throws IOException, InterruptedException {

		int count = 0;
	//	System.out.println("Reducer: " + key.get());
		
		String[] lines = null, splitLine = null; 
		String instanceClass = null;

		double[] InstanceValues = null;
		int[] InstanceIndices = null;

		//System.out.println(dataset.classIndex());

		//if(key.get() == 0){
		for (Text val : values) {
			count++;
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


		}
		System.out.println("Reducer Partition: " + key.get()+" contains: "+ count+" instances");
		count = 0;
	}


	protected void cleanup(Context context) throws IOException, InterruptedException {



	}


}

