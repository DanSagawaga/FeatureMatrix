import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
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


public class JobDriver extends Configured implements Tool {

	static int instanceSize = 0, numFolds = 0, numClasses = 0, numClassifiers = 0, randomSeed = 0;
	static float[] jobTimes = new float[6];
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
	 */
	public static void main(String[] args) {

		System.out.println("The Main Method was Called\n");
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

			ToolRunner.run(new Configuration(), new JobDriver(), args);
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
		 * Job 1 Goes through the DocFeatureSet sequence file to count the number of Documents.
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
		 *  Job 2 reads the DocFreq sequence file and the FeatureSetWeighted sequence file.
		 * 	DocFreqMapper computes the Inverse document frequency for each feature.
		 *	FeatureSetWeightedMapper computes the Term frequency 
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
		 * Job 5 Alpha: Read from Sequence File and do a Cross-Validation on splits of the matrix
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

	public static void initPaths(String[] args)throws Exception{

		File ClassifierModelsDir = new File(args[0]+"/TF_IDF");
		if(!ClassifierModelsDir.exists())
			ClassifierModelsDir.mkdirs();

		docFreqPath = new Path(args[0]+"/feature-sets/ne_all/docfreqs/part-00000");//home/cloudera/Desktop/2-100/feature-sets/ne_all/docfreqs/part-00000			
		featureSetPath = new Path(args[0]+"/feature-sets/ne_all/docfeaturesets-weighted/part-00000");//home/cloudera/Desktop/2-100/feature-sets/ne_all/docfeaturesets-weighted/part-00000
		classMemPath = new Path(args[0]+"/class-memberships/class-memberships.seq");
		outputPath = new Path(args[0]+"/TF_IDF/DocumentCounter");///home/cloudera/Documents/TF_IDF
		outputPath2 = new Path(args[0]+"/TF_IDF/MatrixIntermediateFormat");// /home/cloudera/Documents/TF_IDF
		outputPath3 = new Path(args[0]+"/TF_IDF/MatrixFinalForm");///home/cloudera/Documents/TF_IDF
		outputPath4 = new Path(args[0]+"/TF_IDF/RandomizedMatrices");///home/cloudera/Documents/TF_IDF
		outputPath5 = new Path(args[0]+"/TF_IDF/FinalResults");///home/cloudera/Documents/TF_IDF	

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

	private void  readJob2Output()throws Exception{

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new Path(outputPath2.toString() + "/part-r-00000").toUri(), conf);
		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(new Path(outputPath2.toString() + "/part-r-00000"))));
		String line;
		line=br.readLine();
		String totalFeaturesStr = null, totalRecordsStr = null;
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


	public static void writeTimesToFile()throws IOException{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new Path(existingDirs[0].toString()+"/JobTimes.txt").toUri(), conf);
		PrintWriter writer = new PrintWriter(fs.create(new Path(outputPath5+"/part-r-00000")));
		writer.println("**************************** Total Job Times ****************************");
		for(int k = 0; k < jobTimes.length -1; k++){
			writer.println("Job "+(k+1)+" Time: " + (jobTimes[k]/1000.00)+"s");
		}
		writer.println("Total Jobs Time: " + (jobTimes[jobTimes.length -1]/1000.00)+"s");
		writer.close();
	}
}

