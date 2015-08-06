import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat; 
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import java.io.File;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat;

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

public class HBaseRunner extends Configured implements Tool {

	static Path docFreqPath = null, featureSetPath = null, classMemPath = null, outputPath = null, outputPath2 = null,outputPath3 = null,outputPath4 = null;
	static File existingDirs[] = new File[1];

	static boolean jobsSuccess = false;
	static boolean[] jobsToRun = {true,true,true,true};

	static int instanceSize = 0, numFolds = 0, numClasses = 0;
	static long totalDocuments = 0, totalRecords = 0, totalFeatures = 0, startTime = 0, stopTime = 0, totalStartTime = 0, totalStopTime = 0;

	static Classifier[] models = { new J48(),new PART(),new DecisionTable(),new DecisionStump() ,new NaiveBayes(), new BayesNet(),new KStar(),new ZeroR(),new OneR(),new REPTree()};

	static String parClassifiers = "", docClasses = "";



	public static void main(String[] args) {

		try{
			numFolds = Integer.parseInt(args[1]);

			for(int k = 2; k < args.length; k++)
				parClassifiers += args[k]+ ",";

			if(numFolds < 2){
				System.out.println("Number of folds must be greater than 2!");
				System.out.println("Ending Program.");
				System.exit(-1);
			}

			System.out.println(parClassifiers);

			/*
			 * Instantiates all the output and Input directories needed
			 */
			initPaths(args);
			//Recursively deletes exisiting output directories
			//pickBestModel();
				deleteDirs(existingDirs);

			ToolRunner.run(new Configuration(), new HBaseRunner(), args);

			//	System.exit(jobsSuccess ? 0 : 1);

		}
		catch (Exception e){
			System.out.println("EXCEPTION CAUGHT RED-HANDED: ");
			e.printStackTrace();	
			System.out.println("Ending Program.");
			System.exit(-1);
		}
	}


	public int run(String[] args) throws Exception {

		/*
		 * Job 1 Goes through the DocFeatureSet sequence file to count the number of Documents
		 * to pass to the next job
		 */
		totalStartTime = System.currentTimeMillis();
		Configuration conf = new Configuration();

		if(jobsToRun[0]){
			startTime = System.currentTimeMillis();

			//conf.set("xtinputformat.record.delimiter","</features>");

			Job job1 = Job.getInstance(conf,"Document Counter Job");
			job1.setJarByClass(HBaseRunner.class);
			job1.setJobName("Document Counter Job");

			SequenceFileInputFormat.addInputPath(job1, featureSetPath);
			job1.setMapperClass(DocCounterMapper.class);
			job1.setInputFormatClass(SequenceFileInputFormat.class);
			job1.setReducerClass(DocCounterReducer.class);
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(IntWritable.class);
			LazyOutputFormat.setOutputFormatClass(job1, TextOutputFormat.class);
			FileOutputFormat.setOutputPath(job1, outputPath);
			jobsSuccess = job1.waitForCompletion(true);

			conf.setLong("totalDocuments",totalDocuments);

			stopTime = System.currentTimeMillis();
			System.out.println("********** Job 1 Done. Total Time (ms): " + (stopTime-startTime)+" **********");

		}
		/*
		 * Job 2 reads the DocFreq and the FeatureSetWeighted sequence files
		 * 	DocFreqMapper computes the Inverse document frequency for each feature
		 *	FeatureSetWeighted Mapper computes the Term frequency 
		 *
		 * Both Mappers Pass each feature as a key to the reducer and other relevant data as the value
		 * The Reducer computes the Term Frequency x Inverse Document Frequency and and outputs to a file the result 
		 * The output file consists of lines representing a value in the TFxIDF matrix
		 * Each line will have the format of <Document ID, Feature Index, Feature Name, TFxIDF value>   
		 */

		if(jobsToRun[1] && (totalDocuments > 0)){
			startTime = System.currentTimeMillis();
			//conf	Configuration conf = new Configuration();
			conf.set("xtinputformat.record.delimiter","</features>");
			conf.set("totalDocuments",""+totalDocuments);

			Job job2 = Job.getInstance(conf,"Doc-Feature Matrix Job");
			job2.setJarByClass(HBaseRunner.class);
			job2.setJobName("Doc-Feature Matrix Job");
			job2.setInputFormatClass(SequenceFileInputFormat.class);

			job2.setMapperClass(HbaseDocFreqMapper.class);
			job2.setMapperClass(HBaseMapperFeatureSet.class);
			job2.setReducerClass(TD_IDF_Reducer.class);

			MultipleInputs.addInputPath(job2, docFreqPath, SequenceFileInputFormat.class, HbaseDocFreqMapper.class);
			MultipleInputs.addInputPath(job2,featureSetPath, SequenceFileInputFormat.class, HBaseMapperFeatureSet.class);

			job2.setPartitionerClass(NaturalKeyPartitioner.class);
			job2.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
			job2.setSortComparatorClass(CompositeKeyComparator.class);

			job2.setMapOutputKeyClass(CompositeKey.class);
			job2.setMapOutputValueClass(Text.class);

			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(DoubleWritable.class);

			MultipleOutputs.addNamedOutput(job2, "FeatureIndexKeyText", TextOutputFormat.class, Text.class, DoubleWritable.class);
			MultipleOutputs.addNamedOutput(job2, "Seq",SequenceFileOutputFormat.class,Text.class, DoubleWritable.class);
			//job2.setOutputFormatClass(SequenceFileOutputFormat.class);
			//SequenceFileOutputFormat.setOutputPath(job2, outputPath2);
			FileOutputFormat.setOutputPath(job2, outputPath2);

			jobsSuccess = job2.waitForCompletion(true);

			stopTime = System.currentTimeMillis();
			System.out.println("********** Job 2 Done. Total Time (ms): " + (stopTime-startTime)+" **********");

		}		

		/*
		 * Job 3 Takes the Matrix output of job 2 and groups together all the feature's indices to their respective DocID.
		 * The Doc ID is followed by all its features and their TFxIDF values 
		 * This relational format is needed to use as weka datasets in the next job
		 */
		if(jobsToRun[2]){
			startTime = System.currentTimeMillis();

			conf.setInt("numFolds", numFolds);
			Job job3 = Job.getInstance(conf,"Matrix Output Post-Processing");

			job3.setJarByClass(HBaseRunner.class);
			job3.setJobName("Matrix Output Post-Processing");

			job3.setMapperClass(Job3_Mapper.class);
			job3.setMapperClass(Job3_Mapper_2.class);
			job3.setReducerClass(Job3_Reducer.class);
			job3.setMapOutputKeyClass(CompositeKey.class);
			job3.setMapOutputValueClass(Text.class);

			job3.setPartitionerClass(NaturalKeyPartitioner.class);
			job3.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
			job3.setSortComparatorClass(CompositeKeyComparator.class);

			job3.setInputFormatClass(SequenceFileInputFormat .class);

			outputPath2 = new Path(outputPath2.toString()+"/Seq-r-00000");
			MultipleInputs.addInputPath(job3, outputPath2, SequenceFileInputFormat.class, Job3_Mapper.class);
			MultipleInputs.addInputPath(job3,classMemPath, SequenceFileInputFormat.class, Job3_Mapper_2.class);

			job3.setOutputKeyClass(Text.class);
			job3.setOutputValueClass(Text.class);
			job3.setOutputFormatClass(SequenceFileOutputFormat.class);

			for(int k = 0; k < numFolds; k++)
				MultipleOutputs.addNamedOutput(job3, "MatrixTrainingFold"+k, SequenceFileOutputFormat.class, Text.class, Text.class);

			MultipleOutputs.addNamedOutput(job3, "FullMatrix", TextOutputFormat.class, Text.class, Text.class);
			MultipleOutputs.addNamedOutput(job3, "DocumentClasses", TextOutputFormat.class, Text.class, Text.class);



			FileOutputFormat.setOutputPath(job3, outputPath3);
			SequenceFileOutputFormat.setOutputPath(job3,outputPath3);

			jobsSuccess = job3.waitForCompletion(true);

			stopTime = System.currentTimeMillis();
			System.out.println("********** Job 3 Done. Total Time (ms): " + (stopTime-startTime)+" **********");

		}

		/*
		 * Job 4 Alpha: Read from Sequence File and do a Cross-Validation on splits of the matrix
		 */

		if(jobsToRun[3]){

			/*
			 * readDocumentClasses reads in the different document classes from output file into a string to pass to job 4 
			 */
			docClasses = readDocumentClasses();

			startTime = System.currentTimeMillis();

			conf.setLong("totalFeatures", 2180);
			conf.set("modelsPath", args[0]+"/TF_IDF/ClassifierModels/");
			conf.set("parClassifiers", parClassifiers);
			conf.setInt("numClasses", numClasses);
			conf.set("docClasses",docClasses);

			Job job4 = Job.getInstance(conf,"Nth Split Cross Validation"); 

			job4.setJarByClass(HBaseRunner.class);
			job4.setJobName("Nth Split Cross Validation");

			job4.setInputFormatClass(SequenceFileInputFormat.class);

			for(int k = 0; k < numFolds; k++)
				MultipleInputs.addInputPath(job4, new Path (outputPath3 + "/MatrixTrainingFold"+k+"-r-00000"), SequenceFileInputFormat.class, Job4_Mapper.class);


			job4.setMapOutputKeyClass(IntWritable.class);
			job4.setMapOutputValueClass(Text.class);
			//job4.setNumReduceTasks(0);
			job4.setReducerClass(Job4_Reducer.class);
			job4.setNumReduceTasks(numFolds);

			SequenceFileInputFormat.addInputPath(job4, outputPath3);

			job4.setOutputFormatClass(TextOutputFormat.class);
			FileOutputFormat.setOutputPath(job4, outputPath4);

			//	MultipleOutputs.addNamedOutput(job4, "MatrixFold0",TextOutputFormat.class,Text.class, Text.class);
			//	MultipleOutputs.addNamedOutput(job4, "MatrixFold1",TextOutputFormat.class,Text.class, Text.class);

			jobsSuccess = job4.waitForCompletion(true);
			stopTime = System.currentTimeMillis();
			System.out.println("********** Job 4 Done. Total Time (ms): " + (stopTime-startTime)+" **********");
		}


		if(jobsSuccess){
			totalStopTime = System.currentTimeMillis();
			System.out.println("********** All Jobs Done. Total Time: " + (totalStopTime -totalStartTime)+" **********");
			System.out.println("Number of Documents: "+totalDocuments+
					"\nNumber of Features: "+totalFeatures+
					"\nNumber of Records: "+totalRecords);
			return 0;
		}
		else return 1;
	}


	public static void pickBestModel(){
		System.out.println("Selecting best Model...");

		//	double[] modelPercents = new double[numFolds];
		double[] modelPercents = new double[10];
		String[] modelNames = new String[10];
		int tempCount = 0;
		double max = 0;

		try{
			File dir = new File(outputPath4.toString());

			for (File file : dir.listFiles()) {
				if(file.isFile() && !file.isHidden()){
					String fileName = file.getName();
					String[] splitter = fileName.split("-");
					if(splitter[0].equals("part")){
						Scanner scan = new Scanner(file);
						if(scan.hasNext()){
							modelNames[tempCount] = scan.nextLine();
							splitter = scan.nextLine().split("\t");
							modelPercents[tempCount] = Double.parseDouble(splitter[1]);
							System.out.println(splitter[1]);
						}
						tempCount++;
						scan.close();
					}
				}
			}

		}catch(Exception e){
			e.printStackTrace();
		}
		
		for(int k = 0; k < modelPercents.length; k++){
			if(modelPercents[k] > max)
				max = modelPercents[k];
		}

	}

	public static String readDocumentClasses() throws IOException{
		System.out.println("Reading in document Classes...");
		BufferedReader br = new BufferedReader(new FileReader(outputPath3.toString()+"/DocumentClasses-r-00000"));
		String line = "";
		StringBuilder sb = null;
		try {
			sb = new StringBuilder();
			line = br.readLine();

			while (line != null) {
				sb.append(line);
				sb.append(System.lineSeparator());
				line = br.readLine();
			}

		} catch(Exception e) {
			System.out.println("ERROR: Could not read document classes from file: " + outputPath3.toString()+"/DocumentClasses-r-00000");
			e.printStackTrace();
			System.out.println("Ending Program.");
			System.exit(-1);
		}
		System.out.println("Succesfully read in document Classes...");
		return sb.toString();
	}

	public static void initPaths(String[] args){
		docFreqPath = new Path(args[0]+"/feature-sets/ne_all/docfreqs/part-00000");//home/cloudera/Desktop/2-100/feature-sets/ne_all/docfreqs/part-00000			
		featureSetPath = new Path(args[0]+"feature-sets/ne_all/docfeaturesets-weighted/part-00000");//home/cloudera/Desktop/2-100/feature-sets/ne_all/docfeaturesets-weighted/part-00000
		classMemPath = new Path(args[0]+"/class-memberships/class-memberships.seq");
		outputPath = new Path(args[0]+"/TF_IDF/DocumentCounter");///home/cloudera/Documents/TF_IDF
		outputPath2 = new Path(args[0]+"/TF_IDF/MatrixIntermediateFormat");// /home/cloudera/Documents/TF_IDF
		outputPath3 = new Path(args[0]+"/TF_IDF/MatrixTrainingFolds");///home/cloudera/Documents/TF_IDF
		outputPath4 = new Path(args[0]+"/TF_IDF/ClassifierModels");///home/cloudera/Documents/TF_IDF
		existingDirs[0] = new File(args[0] + "/TF_IDF");
	}

	public static class DocCounterMapper extends Mapper<Text, Text, Text, IntWritable> { 
		public void map(Text DocID, Text line, Context context) throws IOException, InterruptedException {
			//	System.out.println(DocID.toString());
			context.write(DocID, new IntWritable(1));
		}
	}
	public static class DocCounterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) 
				sum += val.get();
			totalDocuments += sum;
		}
	}

	public static class TD_IDF_Reducer extends Reducer<CompositeKey,Text,Text,DoubleWritable>{
		MultipleOutputs<Text, DoubleWritable> mos;


		public void setup(Context context) {
			System.out.println("\n******** Processing TD_IDF_Reducer ********\n");
			mos = new MultipleOutputs(context);
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}

		public void reduce(CompositeKey key, Iterable<Text> valueList, Context context)throws IOException , InterruptedException{
			long recordCount = 0, featureCount = 0;
			String docID = null;
			double IDF = 0.0, TF = 0.0,TF_IDF = 0.0;
			long featureIndex = 0;

			Scanner scan = null;

			//	if(key.getSymbol().equals("11")){

			int count = 0;	
			for(Text value : valueList){
				if(count == 0){
					scan = new Scanner(value.toString());
					scan.useDelimiter("\t");
					if(scan.next().equals("IDF_Flag")){
						featureIndex = Long.parseLong(scan.next());
						IDF = Double.parseDouble(scan.next());
						//	System.out.println("IDF: "+ IDF);
						if(featureIndex > totalFeatures)
							featureCount = featureIndex;
					}
					else{
						System.out.println("Error in Reducer: IDF Key not found for feature: "+key.getPrimaryKey());
						break;
					}	
				}
				else{
					scan = new Scanner(value.toString());
					scan.useDelimiter("\t");
					docID = scan.next();
					TF = Double.parseDouble(scan.next());
					TF_IDF = TF * IDF;
					//System.out.println("DocID: "+docID+" Feature: "+key.getSymbol()+" TF: "+TF+" IDF: "+IDF+" TF_IDF: "+TF_IDF);
					//	System.out.println("DocID: "+docID+" FeatureIndex: "+featureIndex+" Feature Name: "+key.getSymbol()+" TF_IDF: "+TF_IDF);
					mos.write("Seq",new Text(docID+"\t"+featureIndex),new DoubleWritable(TF_IDF));
					//context.write(new Text(docID+"\t"+featureIndex),new DoubleWritable(TF_IDF));
					recordCount++;

				}
				count++;

			}
			totalRecords += recordCount;
			totalFeatures = featureCount;
			mos.write("FeatureIndexKeyText",new Text(key.getPrimaryKey()+ "\t"+featureIndex),null);

		}

	}

	/*
	 * Method is used to delete existing directories that can create a conflict
	 * when Hadoop outputs new data
	 */
	public static void deleteDirs(File[] existingDirs) throws IOException{
		boolean filesExists = false;
		String fileStr = "";
		for(int k = 0; k < existingDirs.length; k++){
			if(existingDirs[k].exists()){
				fileStr += existingDirs[k].toString() + "\n";
				filesExists = true;
			}
		}

		if(filesExists){
			System.out.println("These output directories already exist: " + fileStr);
			System.out.println("Do you want to delete them?  y/n");
			Scanner scan = new Scanner(System.in);
			String input = scan.nextLine();
			if(input.equalsIgnoreCase("y")){
				System.out.println("Deleting existing conflicting directories");
				for(int k = 0; k < existingDirs.length; k++){
					if(existingDirs[k].exists()){
						delete(existingDirs[k]);
					}
				}
			} else{
				System.out.println("Not Deleting Directories");
				//System.exit(0);

			}
		}



	}

	public static void delete(File file)throws IOException{

		if(file.isDirectory()){

			//directory is empty, then delete it
			if(file.list().length==0){

				file.delete();
				//			System.out.println("Directory is deleted : " + file.getAbsolutePath());

			}else{

				//list all the directory contents
				String files[] = file.list();

				for (String temp : files) {
					//construct the file structure
					File fileDelete = new File(file, temp);

					//recursive delete
					delete(fileDelete);
				}

				//check the directory again, if empty then delete it
				if(file.list().length==0){
					file.delete();
					System.out.println("Directory is deleted : " 
							+ file.getAbsolutePath());
				}
			}

		}else{
			//if file, then delete it
			file.delete();
			//	System.out.println("File is deleted : " + file.getAbsolutePath());
		}
	}


}

