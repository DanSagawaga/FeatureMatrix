import java.io.*;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.*;

import org.apache.hadoop.mapreduce.TaskAttemptID;
import weka.classifiers.Classifier;
import weka.classifiers.bayes.*;
import weka.classifiers.lazy.KStar;
import weka.classifiers.rules.*;
import weka.classifiers.trees.*;
import weka.classifiers.functions.*;
import weka.classifiers.rules.DecisionTable ;
import weka.classifiers.meta.*;
import weka.classifiers.pmml.consumer.*;

import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instances;
import weka.core.SparseInstance;
import weka.core.OptionHandler;
import weka.core.converters.ArffSaver;
import weka.classifiers.lazy.*;
import weka.classifiers.UpdateableClassifier;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import weka.classifiers.pmml.consumer.SupportVectorMachineModel
;

public class Job5_Mapper extends Mapper<Text, Text, Text, Text>{

	enum Job5_Mapper_Counter { LINES }
	FastVector<Attribute> fvWekaAttributes = new FastVector<Attribute>();
	FastVector<String> classNominalVal = null;
	static HashMap<String,Double> classNominalMap = null;
	static HashMap<String,String> classifierIndexMap = null;


	int totalDocuments = 0, mapperNum = 0, totalFeatures = 0,numFolds = 0, numDocsInFold = 0, currentPartition = 0,
			trainedDocsCount = 0, mapWriteCount = 0, remainderCount = 0;

	static String outputPath = null, docClasses = null;
	static String[] classifiersToBuildNames = null, classifierOptions = null;
	long docCounter = 0;

	static Instances dataset = null, updatableDataset = null;

	static ArrayList<Classifier> nonUpClassifierList = null;
	static ArrayList<UpdateableClassifier> upClassifierList = null;

	boolean modelsArrayisEmpty = true, updateableClassifiersArrayisEmpty = true;


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

		upClassifierList = new ArrayList<UpdateableClassifier>();
		nonUpClassifierList = new ArrayList<Classifier>();

		indexClassifierBank();
		initClassifiers();
		//checks whether either array is empty
		updateableClassifiersArrayisEmpty = upClassifierList.isEmpty();
		modelsArrayisEmpty = nonUpClassifierList.isEmpty();
		if(modelsArrayisEmpty)
			System.out.println("Non updatable classiffiers array is empty");


		System.out.println("\n****************** Processing Job 5 Mapper: "+mapperNum+ " ******************\n");


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
			classNominalMap.put(splitter[k].trim(), k+0.1);
			//	System.out.println(classNominalMap.get(splitter[k].trim()));
		}

		fvWekaAttributes.addElement( new Attribute("Class Attribute", classNominalVal));
		try{
			dataset = new Instances("FeatureInstance",fvWekaAttributes,fvWekaAttributes.size()-1); 	
			updatableDataset  = new Instances("FeatureInstance",fvWekaAttributes,fvWekaAttributes.size()-1); 
			dataset.setClassIndex(fvWekaAttributes.size()-1);
			updatableDataset.setClassIndex(fvWekaAttributes.size()-1);

			//instantites classifiers in updataeble classifier array

			if(!updateableClassifiersArrayisEmpty){
				System.out.println("Updatable Classifier array is not empty");
				Classifier temp = null;

				for(int k = 0; k < upClassifierList.size();k++){
					temp = (Classifier) upClassifierList.get(k);
					temp.buildClassifier(updatableDataset);
					upClassifierList.set(k, (UpdateableClassifier) temp);
				}

			}

			//	System.out.println(dataset.classAttribute().toString());
			//	System.out.println(dataset.toSummaryString());

		}catch(Exception e){
			e.printStackTrace();
		}

	}

	public void map(Text docID_Class_Text, Text feature_Set, Context context) throws IOException, InterruptedException {

		context.getCounter(Job5_Mapper_Counter.LINES).increment(1);//increments the counter so it can be used as indexer
		docCounter = context.getCounter(Job5_Mapper_Counter.LINES).getValue();

		//	System.out.println("Mapper " + mapperNum + "\t" +docID_Class_Text.toString());

		if(currentPartition != mapperNum && currentPartition < numFolds ){

			String[] lines = null, splitLine = null; 
			String instanceClass = null;

			double[] InstanceValues = null;
			int[] InstanceIndices = null;

			dataset.setClassIndex(fvWekaAttributes.size()-1);

			splitLine = docID_Class_Text.toString().split("\t");
			instanceClass = splitLine[1];

			lines = feature_Set.toString().split("\n");

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


			/*		try{
			System.out.println("IntstanceRow Class: " +instanceRow.toStringNoWeight()+"  Actual Class: "+ instanceClass);
			}catch(Exception e){
				e.printStackTrace();
			}
			 */

			if(!modelsArrayisEmpty)
				dataset.add(instanceRow);

			try{
				if(!updateableClassifiersArrayisEmpty){
					updatableDataset.add(instanceRow);
					for(int k = 0; k < upClassifierList.size();k++){
						upClassifierList.get(k).updateClassifier(updatableDataset.firstInstance());
					}
					updatableDataset.delete();
				}

			}catch(Exception e){
				e.printStackTrace();
			}

			trainedDocsCount++;


		}
		else if(currentPartition < numFolds){
			context.write(new Text(""+mapperNum), new Text(docID_Class_Text.toString() + "\n"+feature_Set.toString()));
			mapWriteCount++;
		}
		/*
		 * The First two mappers write out collectively the entire matrix to the reducers for the test and evaluation phase of the models.
		 * The rest of the mappers write out their remainders that were evenly partitioned in the Job 3 Reducer
		 */

		if(docCounter > numDocsInFold*numFolds){
			remainderCount = (int)(docCounter - (numDocsInFold*numFolds));

			if(remainderCount >(numFolds -1))
				remainderCount = 0;

			if(remainderCount - mapperNum == 0){
				context.write(new Text(""+mapperNum), new Text(docID_Class_Text.toString() + "\n"+feature_Set.toString()));
				mapWriteCount++;	
			}
		}

		if(docCounter%numDocsInFold == 0)
			currentPartition++;

	}
	protected void cleanup(Context context) throws IOException, InterruptedException {
		/*		try{
				System.out.println("Number of Features " + totalFeatures + " size of attribute list " + fvWekaAttributes.size());
					System.out.println(dataset.toString());
		}catch(Exception e){
			e.printStackTrace();
		}
		 */
		System.out.println("Mapper "+ mapperNum + " | Trained "+trainedDocsCount+" | Wrote out "+mapWriteCount + " instances\n");

		try{
			Configuration conf = context.getConfiguration();
			String outputPathStr = conf.get("modelsPath");
			//FileSystem fs = FileSystem.get(new Path(outputPath.toString()+"Models/Mapper_"+mapperNum).toUri(), conf);
			Path outputPath = new Path(outputPathStr+"Models/Mapper_"+mapperNum) ;
			FileSystem fs = FileSystem.get(conf);
			fs.mkdirs(outputPath);


			//	File ClassifierModelsDir = new File(outputPath+"Models/Mapper_"+mapperNum);
			//	ClassifierModelsDir.mkdirs();


			for(int k = 0; k < nonUpClassifierList.size();k++){
				System.out.println("Training classifier "+ nonUpClassifierList.get(k).getClass().getSimpleName()+ " on training fold "+ mapperNum);
				nonUpClassifierList.get(k).buildClassifier(dataset);

			}
			System.out.println("Writing out Classifiers...");
			for(int k = 0; k < nonUpClassifierList.size();k++){
				System.out.println("Writing model "+ nonUpClassifierList.get(k).getClass().getSimpleName());
				fs = FileSystem.get(new Path(outputPath+"/"+nonUpClassifierList.get(k).getClass().getSimpleName()+".model").toUri(), conf);
				FSDataOutputStream out = fs.create(new Path(outputPath.toUri()+"/"+nonUpClassifierList.get(k).getClass().getSimpleName()+".model"));		
				//	System.out.println(outputPath.toUri()+"/"+nonUpClassifierList.get(k).getClass().getSimpleName()+".model");
				weka.core.SerializationHelper.write(out, nonUpClassifierList.get(k));

				out.close();
				//    oos.close();


			}
			for(int k = 0; k < upClassifierList.size();k++){
				System.out.println("Writing model "+ upClassifierList.get(k).getClass().getSimpleName());
				fs = FileSystem.get(new Path(outputPath+"/"+nonUpClassifierList.get(k).getClass().getSimpleName()+".model").toUri(), conf);
				FSDataOutputStream out = fs.create(new Path(outputPath.toUri()+"/"+upClassifierList.get(k).getClass().getSimpleName()+".model"));			
				weka.core.SerializationHelper.write(out, upClassifierList.get(k));
				out.close();

			}

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
		/*
		 * MultilayerPerceptron takes a long time
		 * LMT  gives array index out of bounds exception
		 * SMO gives array index out of bounds exception
		 * AdditiveRegression can't handle binary classes
		 * GaussianProcesses  can't handle binary classes
		 * LinearRegression  can't handle binary classes
		 */

		J48 j48 = null;
		PART part = null;
		DecisionTable decisionTable = null;
		DecisionStump decisionStump = null;
		NaiveBayes naiveBayes = null;
		BayesNet bayesNet = null;
		NaiveBayesMultinomial naiveBayesMultinomial = null;
		OneR oneR = null;
		ZeroR zeroR = null;
		REPTree repTree = null;

		Logistic logistic = null;
		MultilayerPerceptron multilayerPerceptron = null;
		RandomForest randomForest = null;
		SMO sMO = null;
		VotedPerceptron votedPerceptron = null;
		AdditiveRegression additiveRegression = null;
		AttributeSelectedClassifier attributeSelectedClassifier = null;
		ClassificationViaRegression classificationViaRegression = null;
		GaussianProcesses gaussianProcesses = null;
		JRip jRip = null;
		LinearRegression linearRegression = null;
		LMT lMT = null;
		//updatable Classifiers 
		NaiveBayesMultinomialUpdateable NBUM = null;
		NaiveBayesUpdateable NBU = null;
		IBk iBk = null;
		KStar kStar = null;
		LWL lwl = null;
		SGD sGD = null;
		HoeffdingTree hTree = null;
		//SVM Logistic mutlilayerperception randomForest SMO votedPerceptiom 

		try{
			/*
			 * 
			 * Non updataeble Classifiers 
			 * 
			 */

			if(classifierIndexMap.containsKey("Logistic")){
				logistic = new Logistic();
				if(!classifierIndexMap.get("Logistic").equals(""))
					logistic.setOptions(weka.core.Utils.splitOptions(classifierIndexMap.get("Logistic")));
				nonUpClassifierList.add(logistic);
			}		
			if(classifierIndexMap.containsKey("MultilayerPerceptron")){
				multilayerPerceptron = new MultilayerPerceptron();
				if(!classifierIndexMap.get("MultilayerPerceptron").equals(""))
					multilayerPerceptron.setOptions(weka.core.Utils.splitOptions(classifierIndexMap.get("MultilayerPerceptron")));
				nonUpClassifierList.add(multilayerPerceptron);}
			if(classifierIndexMap.containsKey("RandomForest")){
				randomForest = new RandomForest();
				if(!classifierIndexMap.get("RandomForest").equals(""))
					randomForest.setOptions(weka.core.Utils.splitOptions(classifierIndexMap.get("RandomForest")));
				nonUpClassifierList.add(randomForest);}
			if(classifierIndexMap.containsKey("SMO")){
				sMO = new SMO();
				if(!classifierIndexMap.get("SMO").equals(""))
					sMO.setOptions(weka.core.Utils.splitOptions(classifierIndexMap.get("SMO")));
				nonUpClassifierList.add(sMO);}			
			if(classifierIndexMap.containsKey("VotedPerceptron")){
				votedPerceptron = new VotedPerceptron();
				if(!classifierIndexMap.get("VotedPerceptron").equals(""))
					votedPerceptron.setOptions(weka.core.Utils.splitOptions(classifierIndexMap.get("VotedPerceptron")));
				nonUpClassifierList.add(votedPerceptron);}			
			if(classifierIndexMap.containsKey("AttributeSelectedClassifier")){
				attributeSelectedClassifier = new AttributeSelectedClassifier();
				if(!classifierIndexMap.get("AttributeSelectedClassifier").equals(""))
					attributeSelectedClassifier.setOptions(weka.core.Utils.splitOptions(classifierIndexMap.get("AttributeSelectedClassifier")));
				nonUpClassifierList.add(attributeSelectedClassifier);}			
			if(classifierIndexMap.containsKey("AdditiveRegression")){
				additiveRegression = new AdditiveRegression();
				if(!classifierIndexMap.get("AdditiveRegression").equals(""))
					additiveRegression.setOptions(weka.core.Utils.splitOptions(classifierIndexMap.get("AdditiveRegression")));
				nonUpClassifierList.add(additiveRegression);}			
			if(classifierIndexMap.containsKey("ClassificationViaRegression")){
				classificationViaRegression = new ClassificationViaRegression();
				if(!classifierIndexMap.get("ClassificationViaRegression").equals(""))
					classificationViaRegression.setOptions(weka.core.Utils.splitOptions(classifierIndexMap.get("ClassificationViaRegression")));
				nonUpClassifierList.add(classificationViaRegression);}	
			
			if(classifierIndexMap.containsKey("GaussianProcesses")){
				gaussianProcesses = new GaussianProcesses();
				if(!classifierIndexMap.get("GaussianProcesses").equals(""))
					gaussianProcesses.setOptions(weka.core.Utils.splitOptions(classifierIndexMap.get("GaussianProcesses")));
				nonUpClassifierList.add(gaussianProcesses);}
			if(classifierIndexMap.containsKey("JRip")){
				jRip = new JRip();
				if(!classifierIndexMap.get("JRip").equals(""))
					jRip.setOptions(weka.core.Utils.splitOptions(classifierIndexMap.get("JRip")));
				nonUpClassifierList.add(jRip);}
			if(classifierIndexMap.containsKey("LinearRegression")){
				linearRegression = new LinearRegression();
				if(!classifierIndexMap.get("LinearRegression").equals(""))
					linearRegression.setOptions(weka.core.Utils.splitOptions(classifierIndexMap.get("LinearRegression")));
				nonUpClassifierList.add(linearRegression);}

			if(classifierIndexMap.containsKey("LMT")){
				lMT = new LMT();
				if(!classifierIndexMap.get("LMT").equals(""))
					lMT.setOptions(weka.core.Utils.splitOptions(classifierIndexMap.get("LMT")));
				nonUpClassifierList.add(lMT);}

			if(classifierIndexMap.containsKey("J48")){
				j48 = new J48();
				if(!classifierIndexMap.get("J48").equals(""))
					j48.setOptions(weka.core.Utils.splitOptions(classifierIndexMap.get("J48")));
				nonUpClassifierList.add(j48);
			}
			if(classifierIndexMap.containsKey("PART")){
				part = new PART();
				if(!classifierIndexMap.get("PART").equals(""))
					part.setOptions(weka.core.Utils.splitOptions(classifierIndexMap.get("PART")));
				nonUpClassifierList.add(part);
			}
			if(classifierIndexMap.containsKey("DecisionTable")){
				decisionTable = new DecisionTable();
				if(!classifierIndexMap.get("DecisionTable").equals(""))
					decisionTable.setOptions(weka.core.Utils.splitOptions(classifierIndexMap.get("DecisionTable")));
				nonUpClassifierList.add(decisionTable);
			}
			if(classifierIndexMap.containsKey("DecisionStump")){
				decisionStump = new DecisionStump();
				if(!classifierIndexMap.get("DecisionStump").equals(""))
					decisionStump.setOptions(weka.core.Utils.splitOptions(classifierIndexMap.get("DecisionStump")));
				nonUpClassifierList.add(decisionStump);
			}
			if(classifierIndexMap.containsKey("NaiveBayes")){
				naiveBayes = new NaiveBayes();
				if(!classifierIndexMap.get("NaiveBayes").equals(""))
					naiveBayes.setOptions(weka.core.Utils.splitOptions(classifierIndexMap.get("NaiveBayes")));
				nonUpClassifierList.add(naiveBayes);
			}
			if(classifierIndexMap.containsKey("BayesNet")){
				bayesNet = new BayesNet();
				if(!classifierIndexMap.get("BayesNet").equals(""))
					bayesNet.setOptions(weka.core.Utils.splitOptions(classifierIndexMap.get("BayesNet")));
				nonUpClassifierList.add(bayesNet);
			}
			if(classifierIndexMap.containsKey("NaiveBayesMultinomial")){
				naiveBayesMultinomial = new NaiveBayesMultinomial();
				if(!classifierIndexMap.get("NaiveBayesMultinomial").equals(""))
					naiveBayesMultinomial.setOptions(weka.core.Utils.splitOptions(classifierIndexMap.get("NaiveBayesMultinomial")));
				nonUpClassifierList.add( naiveBayesMultinomial);
			}
			if(classifierIndexMap.containsKey("OneR")){
				oneR = new OneR();
				if(!classifierIndexMap.get("OneR").equals(""))
					oneR.setOptions(weka.core.Utils.splitOptions(classifierIndexMap.get("OneR")));
				nonUpClassifierList.add(oneR);
			}

			if(classifierIndexMap.containsKey("ZeroR")){
				zeroR = new ZeroR();
				if(!classifierIndexMap.get("ZeroR").equals(""))
					zeroR.setOptions(weka.core.Utils.splitOptions(classifierIndexMap.get("ZeroR")));
				nonUpClassifierList.add(zeroR);
			}
			if(classifierIndexMap.containsKey("REPTree")){
				repTree = new REPTree();
				if(!classifierIndexMap.get("REPTree").equals(""))
					repTree.setOptions(weka.core.Utils.splitOptions(classifierIndexMap.get("REPTree")));
				nonUpClassifierList.add(repTree);
			}

			/*
			 * 
			 * Updateable Classifiers 
			 * 
			 */
			if(classifierIndexMap.containsKey("NaiveBayesMultinomialUpdateable")){
				NBUM = new NaiveBayesMultinomialUpdateable();
				if(!classifierIndexMap.get("NaiveBayesMultinomialUpdateable").equals(""))
					NBUM.setOptions(weka.core.Utils.splitOptions(classifierIndexMap.get("NaiveBayesMultinomialUpdateable")));
				upClassifierList.add(NBUM);
			}
			if(classifierIndexMap.containsKey("NaiveBayesUpdateable")){
				NBU = new NaiveBayesUpdateable();
				if(!classifierIndexMap.get("NaiveBayesUpdateable").equals(""))
					NBU.setOptions(weka.core.Utils.splitOptions(classifierIndexMap.get("NaiveBayesUpdateable")));
				upClassifierList.add(NBU);
			}
			if(classifierIndexMap.containsKey("IBk")){
				iBk = new IBk();
				if(!classifierIndexMap.get("IBk").equals(""))
					iBk.setOptions(weka.core.Utils.splitOptions(classifierIndexMap.get("IBk")));
				upClassifierList.add(iBk);
			}
			if(classifierIndexMap.containsKey("LWL")){
				lwl = new LWL();
				if(!classifierIndexMap.get("LWL").equals(""))
					lwl.setOptions(weka.core.Utils.splitOptions(classifierIndexMap.get("LWL")));
				upClassifierList.add(lwl);
			}
			if(classifierIndexMap.containsKey("KStar")){
				kStar = new KStar();
				if(!classifierIndexMap.get("KStar").equals(""))
					kStar.setOptions(weka.core.Utils.splitOptions(classifierIndexMap.get("KStar")));
				upClassifierList.add(kStar);
			}
			if(classifierIndexMap.containsKey("SGD")){
				sGD = new SGD();
				if(!classifierIndexMap.get("SGD").equals(""))
					sGD.setOptions(weka.core.Utils.splitOptions(classifierIndexMap.get("SGD")));
				upClassifierList.add(sGD);
			}
			if(classifierIndexMap.containsKey("HoeffdingTree")){
				hTree = new HoeffdingTree();
				if(!classifierIndexMap.get("HoeffdingTree").equals(""))
					hTree.setOptions(weka.core.Utils.splitOptions(classifierIndexMap.get("HoeffdingTree")));
				upClassifierList.add(hTree);
			}

		}catch(Exception e){
			e.printStackTrace();
		}

	}

}
