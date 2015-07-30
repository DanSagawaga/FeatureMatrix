import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configuration;

import weka.classifiers.AggregateableEvaluation;
import weka.classifiers.Classifier;
import weka.classifiers.bayes.BayesNet;
import weka.classifiers.bayes.NaiveBayes;
import weka.classifiers.lazy.IBk;
import weka.classifiers.lazy.KStar;
import weka.classifiers.lazy.LWL;
import weka.classifiers.rules.DecisionTable;
import weka.classifiers.rules.PART;
import weka.classifiers.trees.DecisionStump;
import weka.classifiers.trees.J48;
import weka.core.Attribute;
import weka.core.Instances;
import weka.core.SparseInstance;

import com.google.inject.Key;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;


public class Job4_Reducer extends Reducer <IntWritable,Text,IntWritable,Text> {
	//	MultipleOutputs<Text, DoubleWritable> mos;
	static Instances dataset = null;
	static int totalFeatures = 0;
	static ArrayList<Attribute> attributes = new ArrayList<Attribute>();
	static Classifier[] models = { 
		new J48(),new PART(), new DecisionTable(),new DecisionStump(), //one-level decision tree
		new NaiveBayes(), new BayesNet()
		,new KStar(), new IBk(), new LWL()		
	};
	
	MultipleOutputs<Text, Text> mos;




	public void setup(Context context) {
		System.out.println("\n******** Processing Job 4 Reducer ********\n");
		
		mos = new MultipleOutputs(context);

		/*
		 * Creates Array of attributes to make into the instance data
		 */
		for(int k =0; k < totalFeatures + 1 ; k++)
			attributes.add(new Attribute(""+k));

		ArrayList<String> classifierList = new ArrayList<String>();
		classifierList.add("Rec.Autos");
		classifierList.add("talk.politics.mideast");
		attributes.add(new Attribute("Classifiers",classifierList));	

		try{
			dataset = new Instances("FeatureInstance",attributes,totalFeatures+1);
			dataset.setClassIndex(totalFeatures+1);
		}catch(Exception e){
			e.printStackTrace();
		}

	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
	}



	public void reduce(IntWritable key, Iterable<Text> values, Context context)throws IOException , InterruptedException{

		//System.out.println(Key.get());

		String[] lines = null, splitLine = null; 
		String DocID = null, instanceClass = null, featureStr = "";

		double[] InstanceValues = null;
		int[] InstanceIndices = null;

		SparseInstance sparseRow = null;

		dataset.setClassIndex(totalFeatures+1);
		//System.out.println(dataset.classIndex());

		if(key.get() == 0){
			for (Text val : values) {

				lines = val.toString().split("\n");
				splitLine = lines[0].split("\t");
				DocID = splitLine[0];
				instanceClass = splitLine[1];
				featureStr = "";

				InstanceValues = new double[lines.length-1];
				InstanceIndices = new int[lines.length-1];

				for(int k = 1; k < lines.length; k++){
					splitLine = lines[k].split("\t");
					InstanceIndices[k-1] = Integer.parseInt(splitLine[0]);
					InstanceValues[k-1] = Double.parseDouble(splitLine[1]);
					featureStr += lines[k]+ "\n";
				}

				/*
				 * Builds Instance Row From the Value in the Loop
				 */
				SparseInstance instanceRow = new SparseInstance(1.0,InstanceValues,InstanceIndices,totalFeatures + 1);
				if(instanceClass.equals("rec.autos"))
					instanceRow.setValue(totalFeatures+1, 0.5);
				else	
					instanceRow.setValue(totalFeatures+1, 1.0);

			//	System.out.println(instanceRow.toString());

				dataset.add(instanceRow);	
				mos.write("MatrixFold" + key.get(),new Text(DocID+"\t"+instanceClass),new Text("\n"+featureStr));
				//mos.write("MatrixFold" + key.get(),new Text(""),new Text(val.toString()));

			}
			/*
			 * End of Iterable For loop 
			 */
			try{

				models[key.get()].buildClassifier(dataset);
				dataset.delete();


			}catch (Exception e){
				e.printStackTrace();
			}



		}
	}
}


//	context.write(new IntWritable(DocID.get()), new Text(featureList));
//System.out.println("DocID: " +DocID.toString()+"\n"+ featureList);
//	context.getCounter(Job3_Reducer_Counter.LINES).increment(1);//increments the counter so it can be used as indexer
//	System.out.println(context.getCounter(Job3_Reducer_Counter.LINES).getValue());

//	if(context.getCounter(Job3_Reducer_Counter.LINES).getValue() < totalDocuments/10)
//		System.out.println("First Document: " +context.getCounter(Job3_Reducer_Counter.LINES).getValue());