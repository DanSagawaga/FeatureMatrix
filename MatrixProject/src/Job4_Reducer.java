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

	static int reducerNum = 0, numFolds = 0, keyCount = 0;

	MultipleOutputs<Text, Text> mos;

	public void setup(Context context) {
		TaskAttemptID tid = context.getTaskAttemptID();		
		String[] splitter = tid.toString().split("_");
		reducerNum = Integer.parseInt(splitter[4]);

		System.out.println("\n****************** Processing Job 4 Reducer: "+reducerNum+ " ******************\n");

		Configuration conf = context.getConfiguration();
		numFolds = Integer.parseInt(conf.get("numFolds"));

		mos = new MultipleOutputs<Text,Text>(context);
	}

	public void reduce(Text key, Iterable<Text> values, Context context)throws IOException , InterruptedException{
		String[] splitter = null;
		for(Text value: values){
			splitter = value.toString().split("\n",2);
			for(int k = 0; k < numFolds; k++){
				mos.write("Matrix"+k,new Text(splitter[0]), new Text(splitter[1]));
			}
		}


	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
	}

}