import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat; 
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import java.io.File;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.io.compress.CompressionCodec;
//import org.apache.hadoop.io.compress.CompressionCodecFactory;



/*
 * 		JobDriver.java 
 * 
 * 		Author: Dan Saganome
 * 		August 14, 2015 
 * 
 * 		Summary: This program was created with the purpose of creating a machine learning classifier ensemble trained on sets of pre-proccessed 
 * 		data taken from a set of Documents on MapReduce. The program takes a set of sequence files containing a feature list of a set of documents, 
 * 		as well as the feature frequency within each document and collectively across all documents in order to build a Term Frequency x Inverse Document Frequency 
 * 		sparse-matrix. The matrix is then used to cross-validate a set of classifiers in parallel. The classifier with the best F Measure to each class is written 
 * 		out into a text file with an xml confusion matrix. 
 * 
 * 		Future Work: 
 * 			All Jobs: Use compressed sequence files to reduce the footprint of the program (snappy seems like a good candidate because of its minimal trade-offs)
 * 			Job 5:    Implement a custom writable class that can support serializing objects so the classifier model objects do not have be written and read from HDFS
 * 
 * 		List of Supported Classifiers
 * 		J48 PART DecisionTable DecisionStump NaiveBayes BayesNet NaiveBayesMultinomial OneR ZeroR REPTree LogisticMultilayerPerceptron RandomForestSMO VotedPerceptron 
 * 		AdditiveRegression AttributeSelectedClassifier SGD HoeffdingTree**
 * 
 *       **
 *		  AdditiveRegression can't handle binary classes
 *		  GaussianProcesses  can't handle binary classes
 *		  LinearRegression  can't handle binary classes
 *		  MultilayerPerceptron stalls extendedly
 *		  LMT  gives array index out of bounds exception
 *		  SMO gives array index out of bounds exception
 * 
 */

public class JobDriver extends Configured implements Tool {

	static int instanceSize = 0, numFolds = 0, numClasses = 0, numClassifiers = 0, randomSeed = 0;
	static float[] jobTimes = new float[6];//array to store the different job times
	static long totalDocuments = 0, totalRecords = 0, totalFeatures = 0, startTime = 0, stopTime = 0, totalStartTime = 0, totalStopTime = 0;

	static String parClassifiers = "", docClasses = "";
	static boolean jobsSuccess = false;
	//boolean array to control which jobs to run, for testing purposes
	static boolean[] jobsToRun = {true,true,true,true,true};

	static Path docFreqPath = null, featureSetPath = null, classMemPath = null, outputPath = null,
			outputPath2 = null,outputPath3 = null,outputPath4 = null, outputPath5 = null;
	static File existingDirs[] = null;

	/*
	 * The Main method initializes the main parameters and calls toolRunner to run the jobs
	 * Checks the parameters
	 */
	public static void main(String[] args) {
		try{
			numFolds = Integer.parseInt(args[1]);
			randomSeed = Integer.parseInt(args[2]);
			//Reads in the classifiers into string
			for(int k = 3; k < args.length; k++){
				parClassifiers += args[k]+ "\t";
				numClassifiers++;
			}

			if(numFolds < 2){
				System.out.println("Number of folds must be greater than 2!");
				System.out.println("Ending Program.");
				System.exit(-1);
			}

			/*
			 * Instantiates all the output and Input directories needed
			 */
			initPaths(args);

			//Recursively deletes any existing output directories that will cause a conflict
			deleteDirs(existingDirs);

			ToolRunner.run(new Configuration(), new JobDriver(), args);//runs the jobs

			System.out.println("Number of Documents: "+totalDocuments
					+"\nNumber of Features: "+totalFeatures
					+"\nNumber of Records: "+totalRecords
					+"\nNumber of Classes: "+numClasses);
			writeTimesToFile();
			System.exit(jobsSuccess ? 0 : 1);
		}
		catch (Exception e){
			System.out.println("EXCEPTION CAUGHT: ");
			e.printStackTrace();	
			System.out.println("Ending Program.");
			System.exit(-1);
		}
	}


	public int run(String[] args) throws Exception {

		//Timer for the entire set of jobs
		totalStartTime = System.currentTimeMillis();

		/*
		 * Job 1 Document Counter 
		 * 
		 * Job 1 Goes through the DocFeatureSet sequence file to count the number of Documents.
		 * This number is needed to compute the TFxIDF values in the next Job 
		 */
		Configuration conf = new Configuration();

		if(jobsToRun[0]){
			startTime = System.currentTimeMillis();

			//conf.set("xtinputformat.record.delimiter","</features>");

			Job job1 = Job.getInstance(conf,"Document Counter Job");
			job1.setJobName("Document Counter Job");
			job1.setJarByClass(JobDriver.class);

			job1.setMapperClass(DocCounterMapper.class);
			//	job1.setCombinerClass(DocCounterCombiner.class);
			job1.setReducerClass(DocCounterReducer.class);

			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(IntWritable.class);

			SequenceFileInputFormat.addInputPath(job1, featureSetPath);
			job1.setInputFormatClass(SequenceFileInputFormat.class);

			LazyOutputFormat.setOutputFormatClass(job1, TextOutputFormat.class);
			FileOutputFormat.setOutputPath(job1, outputPath);
			System.out.println("DocCounter Job was called++");
			jobsSuccess = job1.waitForCompletion(true);

			readTotalDocuments();

			stopTime = System.currentTimeMillis();
			jobTimes[0] = (stopTime-startTime);
			System.out.println("********** Job 1 Done. Total Time (ms): " + (stopTime-startTime)+" **********");

		}
		/*
		 * 	Job 2 TFxIDF Matrix Prototype  
		 * 
		 * Job 2 reads the DocFreq sequence file and the FeatureSetWeighted sequence file.
		 * DocFreqMapper computes the Inverse document frequency for each feature.
		 * FeatureSetWeightedMapper computes the Term frequency 
		 *
		 * Both Mappers pass each feature as a key to the reducer and the relevant data as the value
		 * The Reducer computes the Term Frequency x Inverse Document Frequency. 
		 * Secondary sorting is used for this job, explained more in detail in the mappers.
		 * The output file consists of lines representing a value in the TFxIDF matrix
		 * Each line will have the format of <Document ID, Feature Index, Feature Name, TFxIDF value>
		 *   
		 */

		if(jobsToRun[1] && (totalDocuments > 0)){

			startTime = System.currentTimeMillis();
			//sets values needed for the job
			conf.set("totalDocuments",""+totalDocuments);

			Job job2 = Job.getInstance(conf,"TFxIDF Computation");
			job2.setJobName("TFxIDF Computation");
			job2.setJarByClass(JobDriver.class);

			job2.setMapperClass(Job2_DocFreq_Mapper.class);
			job2.setMapperClass(Job2_FeatureSet_Mapper.class);
			job2.setReducerClass(Job2_Reducer.class);

			job2.setInputFormatClass(SequenceFileInputFormat.class);
			MultipleInputs.addInputPath(job2, docFreqPath, SequenceFileInputFormat.class, Job2_DocFreq_Mapper.class);
			MultipleInputs.addInputPath(job2,featureSetPath, SequenceFileInputFormat.class, Job2_FeatureSet_Mapper.class);
			//secondary sorting classes
			job2.setPartitionerClass(NaturalKeyPartitioner.class);
			job2.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
			job2.setSortComparatorClass(CompositeKeyComparator.class);

			job2.setMapOutputKeyClass(CompositeKey.class);
			job2.setMapOutputValueClass(Text.class);

			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(LongWritable.class);
			//Writes a text file with all the features and their assigned indices 
			MultipleOutputs.addNamedOutput(job2, "FeatureIndexKeyText", TextOutputFormat.class, Text.class, DoubleWritable.class);
			MultipleOutputs.addNamedOutput(job2, "Seq",SequenceFileOutputFormat.class,Text.class, DoubleWritable.class);

			FileOutputFormat.setOutputPath(job2, outputPath2);
			//	FileOutputFormat.setCompressOutput(job2, true);
			//	    FileOutputFormat.setOutputCompressorClass(job2, SnappyCodec.class);
			//	    SequenceFileOutputFormat.setOutputCompressionType(job2,CompressionType.BLOCK);

			jobsSuccess = job2.waitForCompletion(true);
			//Reads in the number of features and records counted in the last job
			readJob2Output();
			stopTime = System.currentTimeMillis();
			jobTimes[1] = (stopTime-startTime);
			System.out.println("********** Job 2 Done. Total Time (ms): " + (stopTime-startTime)+" **********");
		}		

		/*
		 * Job 3 Final Matrix Formatting
		 * 
		 * Job 3 Takes the Matrix output of Job 2 and reads from the class membership sequence file to assign each document 
		 * their class. The Job also groups and sorts all the features and their values to each document using secondary sorting.
		 * The Doc ID and its class is followed by all its sorted features and their TFxIDF values 
		 * This relational, sorted, format is required to build Weka data sets to be trained on. 
		 * 
		 */
		if(jobsToRun[2]){
			startTime = System.currentTimeMillis();

			conf.setInt("numFolds", numFolds);
			Job job3 = Job.getInstance(conf,"Matrix Output Post-Processing");

			job3.setJarByClass(JobDriver.class);
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

			//	for(int k = 0; k < numFolds; k++)
			MultipleOutputs.addNamedOutput(job3, "FinalMatrixForm", SequenceFileOutputFormat.class, Text.class, Text.class);

			MultipleOutputs.addNamedOutput(job3, "DocumentClasses", TextOutputFormat.class, Text.class, Text.class);

			FileOutputFormat.setOutputPath(job3, outputPath3);
			SequenceFileOutputFormat.setOutputPath(job3,outputPath3);
			//  FileOutputFormat.setCompressOutput(job3, true);
			// FileOutputFormat.setOutputCompressorClass(job3, SnappyCodec.class);
			//  SequenceFileOutputFormat.setOutputCompressionType(job3,CompressionType.BLOCK);

			jobsSuccess = job3.waitForCompletion(true);

			stopTime = System.currentTimeMillis();
			jobTimes[2] = (stopTime-startTime);
			/*
			 * readDocumentClasses reads in the different document classes from output file into a string to pass to job 4 
			 */
			System.out.println("********** Job 3 Done. Total Time (ms): " + (stopTime-startTime)+" **********");

		}
		/*
		 * Job 4 Matrix Data Randomizer 
		 * 
		 * Job 4 simply randomizes the matrix according to the inputted random seed number.
		 * The reducer makes n (fold number) identical copies of the matrix to be read by n number of mappers in the next job
		 * 
		 * **Future Work** A useful feature that can be added to this job is to also stratify the matrix in order have 
		 * ** the ability to have stratified cross validation.
		 */

		if(jobsToRun[3]){
			startTime = System.currentTimeMillis();
			System.out.println("\nRandom Seed is "+randomSeed+"\n\n");
			conf.setInt("randomSeed",randomSeed);
			Job job4 = Job.getInstance(conf,"Matrix Randomizer Job");

			job4.setJarByClass(JobDriver.class);
			job4.setJobName("Matrix Output Post-Processing");

			job4.setMapperClass(Job4_Mapper.class);
			job4.setMapperClass(Job4_Mapper.class);
			job4.setReducerClass(Job4_Reducer.class);
			job4.setMapOutputKeyClass(Text.class);
			job4.setMapOutputValueClass(Text.class);

			Path tempOutputPath3 = new Path(outputPath3.toString()+"/FinalMatrixForm-r-00000");
			job4.setInputFormatClass(SequenceFileInputFormat.class);
			SequenceFileInputFormat.addInputPath(job4, tempOutputPath3);

			job4.setOutputKeyClass(Text.class);
			job4.setOutputValueClass(Text.class);
			job4.setOutputFormatClass(SequenceFileOutputFormat.class);
			/*
			 * Creates n (number of folds) copies of the matrix to be read by the next job
			 */
			for(int k = 0; k < numFolds; k++)
				MultipleOutputs.addNamedOutput(job4, "Matrix"+k, SequenceFileOutputFormat.class, Text.class, Text.class);

			FileOutputFormat.setOutputPath(job4, outputPath4);
			SequenceFileOutputFormat.setOutputPath(job4,outputPath4);
			//  FileOutputFormat.setCompressOutput(job3, true);
			// FileOutputFormat.setOutputCompressorClass(job3, SnappyCodec.class);
			//  SequenceFileOutputFormat.setOutputCompressionType(job3,CompressionType.BLOCK);

			jobsSuccess = job4.waitForCompletion(true);
			stopTime = System.currentTimeMillis();
			jobTimes[3] = (stopTime-startTime);
			System.out.println("********** Job 4 Done. Total Time (ms): " + (stopTime-startTime)+" **********");
		}

		/*
		 * Job 5 Parallel Cross-Validation Training/Testing 
		 * 
		 * Job 5 performs a cross validation of every classifier in parallel and picks the classifier that achieved the highest F Score
		 * Job 5 has n (number of Folds) mappers that each read a copy of the matrix. Each mapper trains each classifier on a different
		 * permutation of the matrix and write out to its combiner the fold that it did not train on, ie. the test dataset.
		 * For example, mapper 0 will train on all folds except for 0 and write out fold 0 to its combiner, mapper 1 will train on all 
		 * folds except for fold 1 and write fold 1 to its combiner for testing...
		 * Once a mapper is done training, it writes it's trained classifier models to HDFS in a directory named after its number (mapper_0)
		 * Once the Combiner starts, it first reads in the classifier models from HDFS and initializes them into an array of classifiers.
		 * The classifiers are then validated on the test fold taken from the its mapper and all the relevant result information such as all
		 * the F measures of each class and the confusion matrices are passes as a string to the reducer. 
		 * In the reducer the classifiers are grouped by their name in which a comparison of their F measures is stored and compared to in ordered to select
		 * which classifier achieved the highest F measure for each class. That information is then written to a text file which listing which classifiers where 
		 * the best in classifying each class, what training fold it trained on, and the confusion matrix from the test fold.
		 * 
		 */

		if(jobsToRun[4]){

			startTime = System.currentTimeMillis();
			docClasses = readDocumentClasses();
			System.out.println("Features: " + totalFeatures + "  " + "Classes "+ docClasses);

			conf.setLong("totalFeatures", totalFeatures);
			conf.set("modelsPath", args[0]+"/TF_IDF/ClassifierModels/");
			conf.set("parClassifiers", parClassifiers);
			conf.setInt("numClasses", numClasses);
			conf.set("docClasses",docClasses);


			Job job5 = Job.getInstance(conf,"Nth Split Cross Validation"); 

			job5.setJarByClass(JobDriver.class);
			job5.setJobName("Nth Split Cross Validation");

			job5.setInputFormatClass(SequenceFileInputFormat.class);

			for(int k = 0; k < numFolds; k++)
				MultipleInputs.addInputPath(job5, new Path (outputPath4 + "/Matrix"+k+"-r-00000"), SequenceFileInputFormat.class, Job5_Mapper.class);


			job5.setMapOutputKeyClass(Text.class);
			job5.setMapOutputValueClass(Text.class);
			//job5.setNumReduceTasks(0);
			job5.setCombinerClass(Job5_Combiner.class);
			job5.setReducerClass(Job5_Reducer.class);
			//job5.setNumReduceTasks(numFolds);

			SequenceFileInputFormat.addInputPath(job5, outputPath4);

			job5.setOutputFormatClass(TextOutputFormat.class);
			FileOutputFormat.setOutputPath(job5, outputPath5);

			//for(int k = 0; k < numFolds; k++)
			//	MultipleOutputs.addNamedOutput(job5, "ReducerResult"+k,TextOutputFormat.class,Text.class, Text.class);
			//	MultipleOutputs.addNamedOutput(job5, "MatrixFold1",TextOutputFormat.class,Text.class, Text.class);

			jobsSuccess = job5.waitForCompletion(true);
			stopTime = System.currentTimeMillis();
			jobTimes[4] = (stopTime-startTime);
			System.out.println("********** Job 5 Done. Total Time (ms): " + (stopTime-startTime)+" **********");
		}


		if(jobsSuccess){
			totalStopTime = System.currentTimeMillis();
			jobTimes[5] = (totalStopTime -totalStartTime);
			System.out.println("********** All Jobs Done. Total Time: " + (totalStopTime -totalStartTime)+" **********");
			return 0;
		}
		else return 1;
	}


	public static String readDocumentClasses() throws IOException{
		System.out.println("Reading in document Classes...");

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new Path(outputPath3.toString() +"/DocumentClasses-r-00000").toUri(), conf);
		BufferedReader br =new BufferedReader(new InputStreamReader(fs.open(new Path(outputPath3.toString() +"/DocumentClasses-r-00000"))));
		String line = "", classStr = "";
		line=br.readLine();
		while(line != null){
			classStr += line.trim() + "\n";
			numClasses++;
			line=br.readLine();
		}
		return classStr; 


	}
	/*
	 * Initializes all the paths used by the jobs. Since there are multiple outputs, the output path are hard coded to be relative to 
	 * the path inputed by the user.
	 */
	public static void initPaths(String[] args)throws Exception{

		//makes the parent directory for all the outputs
		File ClassifierModelsDir = new File(args[0]+"/TF_IDF");
		if(!ClassifierModelsDir.exists())
			ClassifierModelsDir.mkdirs();

		docFreqPath = new Path(args[0]+"/feature-sets/ne_all/docfreqs/part-00000");		
		featureSetPath = new Path(args[0]+"/feature-sets/ne_all/docfeaturesets-weighted/part-00000");
		classMemPath = new Path(args[0]+"/class-memberships/class-memberships.seq");
		outputPath = new Path(args[0]+"/TF_IDF/DocumentCounter");
		outputPath2 = new Path(args[0]+"/TF_IDF/MatrixIntermediateFormat");
		outputPath3 = new Path(args[0]+"/TF_IDF/MatrixFinalForm");
		outputPath4 = new Path(args[0]+"/TF_IDF/RandomizedMatrices");
		outputPath5 = new Path(args[0]+"/TF_IDF/FinalResults");			
		/*
		 * Path to check if this directory exists on the start of the program.
		 * If it exists then the main method calls the deleteDirs method to recursively
		 * delete the directory to prevent conflicts.
		 */
		existingDirs = new File[1];
		existingDirs[0] = new File(args[0] + "/TF_IDF");
	}

	private void  readTotalDocuments()throws Exception{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new Path(outputPath.toString() + "/part-r-00000").toUri(), conf);
		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(new Path(outputPath.toString() + "/part-r-00000"))));
		String line;
		line=br.readLine();
		while (line != null){
			if(line != "")
				totalDocuments = Integer.parseInt(line.trim());
			line=br.readLine();
		}

	}
	/*
	 * Reads in the number of Features, and Records for future user
	 */
	private void  readJob2Output()throws Exception{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new Path(outputPath2.toString() + "/part-r-00000").toUri(), conf);
		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(new Path(outputPath2.toString() + "/part-r-00000"))));
		String line;
		line=br.readLine();
		for(int k = 0;line != null; k++){
			if(k == 1)
				totalFeatures = (int)Double.parseDouble(line.trim());
			if(k == 2)
				totalRecords = (int) Double.parseDouble(line.trim());

			line=br.readLine();
		}

	}

	/*
	 * Method is used to delete existing directories that can create a conflict
	 * when Hadoop outputs new data
	 */
	public static void deleteDirs(File[] existingDirs) throws IOException{

		Configuration conf = new Configuration(); 
		conf.addResource(existingDirs[0].getPath());	
		FileSystem fileSystem = FileSystem.get(conf);

		String file = existingDirs[0].toString();
		Path path = new Path(existingDirs[0].toString());
		if (!fileSystem.exists(path)) {
			System.out.println("File " + file + " does not exists");
			System.out.println("Not Deleting Directories");
			return;
		}
		else{
			System.out.println("These output directories already exist: " + path.toString());
			System.out.println("Do you want to delete them?  y/n");
			Scanner scan = new Scanner(System.in);
			String input = scan.nextLine();
			if(input.equalsIgnoreCase("y")){
				System.out.println("Deleting existing conflicting directories");
				fileSystem.delete(new Path(file), true);
				fileSystem.close();	

			}
			scan.close();
		}
	}



	/*
	 * Writes the final times of the jobs to a file in the Final Result Directory 
	 */
	public static void writeTimesToFile()throws IOException{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new Path(outputPath5.toString()+"/JobTimes.txt").toUri(), conf);
		PrintWriter writer = new PrintWriter(fs.create(new Path(outputPath5+"/JobTimes.txt")));
		writer.println("**************************** Total Job Times ****************************");
		for(int k = 0; k < jobTimes.length -1; k++){
			writer.println("Job "+(k+1)+" Time: " + (jobTimes[k]/1000.00)+"s");
		}
		writer.println("Total Jobs Time: " + (jobTimes[jobTimes.length -1]/1000.00)+"s");
		writer.close();
	}
}

