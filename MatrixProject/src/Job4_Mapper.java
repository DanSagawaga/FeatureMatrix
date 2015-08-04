import java.io.File;
import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.input.SAXBuilder;
import org.apache.hadoop.io.WritableComparable;
import java.util.*;

import org.apache.hadoop.mapreduce.TaskAttemptID;
import weka.classifiers.Classifier;
import weka.classifiers.bayes.BayesNet;
import weka.classifiers.bayes.NaiveBayes;
import weka.classifiers.lazy.KStar;
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
public class Job4_Mapper extends Mapper<Text, Text, IntWritable, Text>{

    enum Job4_Mapper_Counter { LINES }
	FastVector<Attribute> fvWekaAttributes = new FastVector<Attribute>();

    static int totalDocuments = 0, mapperNum = 0, totalFeatures = 0,numFolds = 0, numDocsInFold = 0, currentPartition = 0, mapperClassifierIndex = 0, tempCount = 0;;
	int[] mapWriteCount = new int[10];
	
	static String outputPath = null, mapperClassifierStr = null;
	long docCounter = 0;
	
	static Instances dataset = null, tempSet = null;

	static Classifier[] models = { new J48(),new PART(),new DecisionTable(),new DecisionStump() ,new NaiveBayes(), new BayesNet(),new KStar(),new ZeroR(),new OneR(),new REPTree()};


	public void setup(Context context) {
		TaskAttemptID tid = context.getTaskAttemptID();		
		String[] splitter = tid.toString().split("_");
		mapperNum = Integer.parseInt(splitter[4]);
		
		
		Configuration conf = context.getConfiguration();
		totalFeatures = Integer.parseInt(conf.get("totalFeatures"));
		totalDocuments= Integer.parseInt(conf.get("totalDocuments"));
		numFolds = Integer.parseInt(conf.get("numFolds"));
		numDocsInFold = (int)(totalDocuments/numFolds);
		
		mapperClassifierStr = conf.get("parClassifiers");
		mapperClassifierIndex = getClassifierIndex(mapperClassifierStr);
		

		if(mapperNum == 0)
			currentPartition = 1;

		System.out.println("\n******** Processing Job 4 Mapper: "+mapperNum+ " ********\n");


		/*
		 * Creates Array of attributes to make into the instance data
		 */
		for(int k =0; k < totalFeatures; k++)
			fvWekaAttributes.addElement(new Attribute("Feature "+ k));


		FastVector<String> fvNominalVal = new FastVector<String>(2);
		fvNominalVal.addElement("Rec.Autos");
		fvNominalVal.addElement("talk.politics.mideast");

		fvWekaAttributes.addElement( new Attribute("Class Attribute", fvNominalVal));
		try{
			dataset = new Instances("FeatureInstance",fvWekaAttributes,fvWekaAttributes.size()-1); 	
			tempSet = new Instances(dataset);

			dataset.setClassIndex(fvWekaAttributes.size()-1);
	//		System.out.println(dataset.classAttribute().toString());
			//	System.out.println(dataset.toSummaryString());

		}catch(Exception e){
			e.printStackTrace();
		}

	}

	public void map(Text docID_Class_Text, Text feature_Set, Context context) throws IOException, InterruptedException {

		context.getCounter(Job4_Mapper_Counter.LINES).increment(1);//increments the counter so it can be used as indexer
		docCounter = context.getCounter(Job4_Mapper_Counter.LINES).getValue();
		
	//	System.out.println("Mapper " + mapperNum + "\t" +docID_Class_Text.toString());

		
		String[] lines = null, splitLine = null; 
		String instanceClass = null;

		double[] InstanceValues = null;
		int[] InstanceIndices = null;

		dataset.setClassIndex(fvWekaAttributes.size()-1);

		splitLine = docID_Class_Text.toString().split("\t");
		instanceClass = splitLine[1];

		lines = feature_Set.toString().split("\n");

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

		InstanceValues = new double[lines.length];
		InstanceIndices = new int[lines.length];

		for(int k = 0; k < lines.length; k++){
			splitLine = lines[k].split("\t");
			InstanceIndices[k] = Integer.parseInt(splitLine[0]);
			InstanceValues[k] = Double.parseDouble(splitLine[1]);
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

		/*
		 * The First two mappers write out collectively the entire matrix to the reducers for the test and evaluation phase of the models.
		 * The rest of the mappers write out their remainders that were evenly partitioned in the Job 3 Reducer
		 */
		if(mapperNum == 0 && currentPartition != 10){
			if(docCounter%numDocsInFold != 0){
				context.write(new IntWritable(currentPartition), new Text(docID_Class_Text.toString() + "\n"+feature_Set.toString()));
				mapWriteCount[mapperNum]++;
			}
			else{
				context.write(new IntWritable(currentPartition), new Text(docID_Class_Text.toString() + "\n"+feature_Set.toString()));
				currentPartition++;
				mapWriteCount[mapperNum]++;
			}
		}
		/*
		 * Writes out the first fold of the matrix only because it is the only fold missing from the first mapper
		 */
		else if(mapperNum == 1){
			if(( docCounter%numDocsInFold != 0) && currentPartition == 0){

				context.write(new IntWritable(currentPartition), new Text(docID_Class_Text.toString() + "\n"+feature_Set.toString()));
				mapWriteCount[mapperNum]++;
			}
			else if(currentPartition == 0){
				context.write(new IntWritable(currentPartition), new Text(docID_Class_Text.toString() + "\n"+feature_Set.toString()));
				currentPartition++;
				mapWriteCount[mapperNum]++;
			}
		}
		else if(docCounter > numDocsInFold * (numFolds -1)){
			context.write(new IntWritable(mapperNum), new Text(docID_Class_Text.toString() + "\n"+feature_Set.toString()));
			mapWriteCount[mapperNum]++;
		}


	}
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		System.out.println("Mapper "+ mapperNum + " | wrote out "+mapWriteCount[mapperNum] + " instances");
		
		Configuration conf = context.getConfiguration();
		outputPath = conf.get("modelsPath");
		File ClassifierModelsDir = new File(outputPath+"Models/");
		ClassifierModelsDir.mkdirs();

		try{
			models[mapperClassifierIndex].buildClassifier(dataset);
			//System.out.println("Classifier: " + key.get()+ " " +models[key.get()].toSummaryString());
			weka.core.SerializationHelper.write(outputPath+"Models/"+mapperNum +"_"+mapperClassifierStr+".model", models[mapperClassifierIndex]);
			//System.out.println(dataset.toString());
			dataset.delete();



		}catch (Exception e){
			e.printStackTrace();
		}
	//	System.out.println(context.getCounter(Job4_Mapper_Counter.LINES).getValue());
	}
	
	public static int getClassifierIndex(String classifierPar){
		int index = -1;
		String splitter[] = classifierPar.split(" ");
		mapperClassifierStr = splitter[mapperNum];
		
		for(int k = 0; k < models.length; k++){
			if(mapperClassifierStr.equals(models[k].getClass().getSimpleName()))
					index = k;
		}
		if(index == -1){
			System.out.println("Could not find classifier name: "+ mapperClassifierStr+ " Defaulting to classifer: "+ models[0].getClass().getSimpleName());
		index = 0;	
		}
		return index;
	}
}
