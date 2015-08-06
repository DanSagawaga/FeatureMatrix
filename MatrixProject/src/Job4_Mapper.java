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
import weka.core.OptionHandler;
public class Job4_Mapper extends Mapper<Text, Text, IntWritable, Text>{

	enum Job4_Mapper_Counter { LINES }
	FastVector<Attribute> fvWekaAttributes = new FastVector<Attribute>();
	FastVector<String> classNominalVal = null;
	static HashMap<String,Double> classNominalMap = null;
	static HashMap<String,String> classifierIndexMap = null;


	int totalDocuments = 0, mapperNum = 0, totalFeatures = 0,numFolds = 0, numDocsInFold = 0, currentPartition = 0, tempCount = 0;;
	int[] mapWriteCount = new int[10];

	static String outputPath = null, docClasses = null;
	static String[] classifiersToBuildNames = null, classifierOptions = null;
	long docCounter = 0;

	static Instances dataset = null, tempSet = null;

	static Classifier[] models = null;// { new J48(),new PART(),new DecisionTable(),new DecisionStump() ,new NaiveBayes(), new BayesNet(),new KStar(),new ZeroR(),new OneR(),new REPTree()};


	public void setup(Context context) {
		TaskAttemptID tid = context.getTaskAttemptID();		
		String[] splitter = tid.toString().split("_");
		mapperNum = Integer.parseInt(splitter[4]);


		Configuration conf = context.getConfiguration();
		docClasses = conf.get("docClasses");
		totalFeatures = Integer.parseInt(conf.get("totalFeatures"));
		totalDocuments= Integer.parseInt(conf.get("totalDocuments"));
		numFolds = Integer.parseInt(conf.get("numFolds"));
		numDocsInFold = (int)(totalDocuments/numFolds);

		classifiersToBuildNames = conf.get("parClassifiers").split("\t");
		indexClassifierBank();
		initClassifiers();

		
		System.out.println("\n****************** Processing Job 4 Mapper: "+mapperNum+ " ******************\n");


		/*
		 * Creates Array of attributes to make into the instance data
		 */
		for(int k =0; k < totalFeatures; k++)
			fvWekaAttributes.addElement(new Attribute("Feature "+ k));

		splitter = docClasses.split("\n");
		classNominalVal = new FastVector<String>(splitter.length);
		classNominalMap = new HashMap<String,Double>(splitter.length);
		/*
		 * populates FastVector list to add as class attribute to the dataset
		 * populates HashMap to compare each document's class 
		 */
		for(int k = 0; k < splitter.length; k++){
			classNominalVal.addElement(splitter[k].trim());
			classNominalMap.put(splitter[k].trim(), 1.0 / (k+1.0));
			System.out.println(classNominalMap.get(splitter[k].trim()));
		}

		fvWekaAttributes.addElement( new Attribute("Class Attribute", classNominalVal));
		try{
			dataset = new Instances("FeatureInstance",fvWekaAttributes,fvWekaAttributes.size()-1); 	
			//tempSet = new Instances(dataset);

			dataset.setClassIndex(fvWekaAttributes.size()-1);
			System.out.println(dataset.classAttribute().toString());
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

		instanceRow.setValue(fvWekaAttributes.size()-1, classNominalMap.get(instanceClass));


		dataset.add(instanceRow);	

		/*
		 * The First two mappers write out collectively the entire matrix to the reducers for the test and evaluation phase of the models.
		 * The rest of the mappers write out their remainders that were evenly partitioned in the Job 3 Reducer
		 */
		if(mapperNum==9){
			if(docCounter%numDocsInFold != 0){
				if(mapperNum - 1 == currentPartition){
					context.write(new IntWritable(0), new Text(docID_Class_Text.toString() + "\n"+feature_Set.toString()));
					mapWriteCount[mapperNum]++;
				}
			}
			else{
				if(mapperNum -1 == currentPartition-1){
					context.write(new IntWritable(0), new Text(docID_Class_Text.toString() + "\n"+feature_Set.toString()));
					mapWriteCount[mapperNum]++;
				}
				currentPartition++;
			}
		}

		else if(docCounter%numDocsInFold != 0){
			if(mapperNum == currentPartition){
				context.write(new IntWritable(currentPartition+1), new Text(docID_Class_Text.toString() + "\n"+feature_Set.toString()));
				mapWriteCount[mapperNum]++;
			}
		}
		else{
			if(mapperNum == currentPartition){
				context.write(new IntWritable(currentPartition+1), new Text(docID_Class_Text.toString() + "\n"+feature_Set.toString()));
				mapWriteCount[mapperNum]++;
			}
			currentPartition++;
		}

		if(docCounter > numDocsInFold * (numFolds -1)){
			context.write(new IntWritable(mapperNum), new Text(docID_Class_Text.toString() + "\n"+feature_Set.toString()));
			mapWriteCount[mapperNum]++;
		}


	}
	protected void cleanup(Context context) throws IOException, InterruptedException {

		System.out.println("Mapper "+ mapperNum + " | wrote out "+mapWriteCount[mapperNum] + " instances\n");

		Configuration conf = context.getConfiguration();
		outputPath = conf.get("modelsPath");
		File ClassifierModelsDir = new File(outputPath+"Models/Mapper_"+mapperNum);
		ClassifierModelsDir.mkdirs();

		try{
			for(int k = 0; k < models.length;k++)
				models[k].buildClassifier(dataset);
			//System.out.println("Classifier: " + key.get()+ " " +models[key.get()].toSummaryString());
			for(int k = 0; k < models.length;k++){
				weka.core.SerializationHelper.write(outputPath+"Models/Mapper_"+mapperNum +"/"+models[k].getClass().getSimpleName()+".model", models[k]);
			}
			//System.out.println(dataset.toString());
			dataset.delete();



		}catch (Exception e){
			e.printStackTrace();
		}
		//	System.out.println(context.getCounter(Job4_Mapper_Counter.LINES).getValue());
	}

	/*
	 * Populates Map with the name of the classifiers to be used as the key
	 * and the option parameters as the values.
	 * This map is used to initialize the specific classifiers with their 
	 * options in the initClassifiers method.
	 */
	public static void indexClassifierBank(){

		String[] splitter = null;
		classifierIndexMap = new HashMap<String,String>(classifiersToBuildNames.length);

		for(int k = 0; k < classifiersToBuildNames.length; k++){
			splitter = classifiersToBuildNames[k].split(",");
			if(splitter.length > 1)
			classifierIndexMap.put(splitter[0],splitter[1]);
			else
				classifierIndexMap.put(splitter[0],"");
		}

	}

	public static void initClassifiers(){
		
		models = new Classifier[classifiersToBuildNames.length];
		int modelsIndex = 0;
		J48 j48 = null;
		PART part = null;
		DecisionTable decisionTable = null;
		DecisionStump decisionStump = null;
		NaiveBayes naiveBayes = null;
		BayesNet bayesNet = null;
		KStar kStar = null;
		OneR oneR = null;
		ZeroR zeroR = null;
		REPTree repTree = null;

		try{
			if(classifierIndexMap.containsKey("J48")){
				j48 = new J48();
				j48.setOptions(weka.core.Utils.splitOptions(classifierIndexMap.get("J48")));
				System.out.println("Training on J48 with options: "+Arrays.toString(j48.getOptions()));
				models[modelsIndex] = j48;
				modelsIndex++;
			}
			if(classifierIndexMap.containsKey("PART")){
				part = new PART();
				part.setOptions(weka.core.Utils.splitOptions(classifierIndexMap.get("PART")));
				models[modelsIndex] = part;
				modelsIndex++;
			}
			if(classifierIndexMap.containsKey("DecisionTable")){
				decisionTable = new DecisionTable();
				decisionTable.setOptions(weka.core.Utils.splitOptions(classifierIndexMap.get("DecisionTable")));
				models[modelsIndex] = decisionTable;
				modelsIndex++;
			}
			if(classifierIndexMap.containsKey("DecisionStump")){
				decisionStump = new DecisionStump();
				decisionStump.setOptions(weka.core.Utils.splitOptions(classifierIndexMap.get("DecisionStump")));
				models[modelsIndex] = decisionStump;
				modelsIndex++;
			}
			if(classifierIndexMap.containsKey("NaiveBayes")){
				naiveBayes = new NaiveBayes();
				naiveBayes.setOptions(weka.core.Utils.splitOptions(classifierIndexMap.get("NaiveBayes")));
				models[modelsIndex] = naiveBayes;
				modelsIndex++;
			}
			if(classifierIndexMap.containsKey("BayesNet")){
				bayesNet = new BayesNet();
				bayesNet.setOptions(weka.core.Utils.splitOptions(classifierIndexMap.get("BayesNet")));
				models[modelsIndex] = bayesNet;
				modelsIndex++;
			}
			if(classifierIndexMap.containsKey("OneR")){
				oneR = new OneR();
				oneR.setOptions(weka.core.Utils.splitOptions(classifierIndexMap.get("OneR")));
				models[modelsIndex] = oneR;
				modelsIndex++;
			}
			if(classifierIndexMap.containsKey("KStar")){
				kStar = new KStar();
				kStar.setOptions(weka.core.Utils.splitOptions(classifierIndexMap.get("KStar")));
				models[modelsIndex] = kStar;
				modelsIndex++;
			}
			if(classifierIndexMap.containsKey("ZeroR")){
				zeroR = new ZeroR();
				zeroR.setOptions(weka.core.Utils.splitOptions(classifierIndexMap.get("ZeroR")));
				models[modelsIndex] = zeroR;
				modelsIndex++;
			}
			if(classifierIndexMap.containsKey("REPTree")){
				repTree = new REPTree();
				repTree.setOptions(weka.core.Utils.splitOptions(classifierIndexMap.get("REPTree")));
				models[modelsIndex] = repTree;
				modelsIndex++;
			}


		}catch(Exception e){
			e.printStackTrace();
		}

	}

	public static void setOptions(String[] classifiersPar){

		String[] splitter = null;
		for(int k = 0; k < classifiersToBuildNames.length; k++){
			splitter = classifiersToBuildNames[k].split(",");
			classifiersToBuildNames[k] = splitter[0];
			//OptionHandler x = new OptionHandler();

			//	(new OptionHandler) models[classifierIndexMap.get(classifiersToBuildNames[k])] = null;



		}
		//	models[classifierIndexMap.get(classifiersToBuildNames[k])]

	}

}
