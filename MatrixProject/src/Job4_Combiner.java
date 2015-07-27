import java.io.IOException;

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

public class Job4_Combiner extends Reducer<IntWritable,Text,IntWritable,Text> {

	static long totalFeatures = 0;
	static double[] instanceRow;
	public void setup(Context context){
		System.out.println("\n******** Processing Job 4 Combiner ********\n");
		Configuration conf = context.getConfiguration();
		totalFeatures = Long.parseLong(conf.get("totalFeatures"));
		instanceRow = new double[totalFeatures];

		
	}


	public void reduce(IntWritable key, Iterable<Text> values,  Context context) throws IOException, InterruptedException {

		SparseInstance temp = new 
		
		for (Text val : values) {
		}
		System.out.println(key.toString());


	}
}
