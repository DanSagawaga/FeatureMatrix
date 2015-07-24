import java.io.IOException;
import java.util.Scanner;

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


public class HBaseRunner  {
	static long totalDocuments = 0, totalRecords = 0, totalFeatures = 0;
	static boolean jobsSuccess = false;
	static int splitsNum = 0;

	public static void main(String[] args) {
		try{
			if (args.length != 3) {
				System.out.printf(
						"\nNOT RIGHT AMOUNT OF ARGUMENTS\n");
				System.exit(-1);
			}

			Path docFreqPath = new Path(args[0]);//home/cloudera/Desktop/2-100/feature-sets/ne_all/docfreqs/part-00000			
			Path featureSetPath = new Path(args[1]);//home/cloudera/Desktop/2-100/feature-sets/ne_all/docfeaturesets-weighted/part-00000
			Path crossValPath = new Path(args[2]);
			Path outputPath = new Path("/home/cloudera/Documents/TF_IDF");
			Path outputPath2 = new Path("/home/cloudera/Documents/TF_IDF2");
			Path outputPath3 = new Path("/home/cloudera/Documents/TF_IDF3");
			
			/*
			 * Job 1 Goes through the DocFeatureSet sequence file to count the number of Documents
			 * to pass to the next job
			 */
	// /*		
			Configuration conf = new Configuration();
			conf.set("xtinputformat.record.delimiter","</features>");
			conf.set("totalDocuments","Dan is Great");


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
// /*
			if(job1.waitForCompletion(true) && totalDocuments > 0){
				Configuration conf2 = new Configuration();
				conf2.set("xtinputformat.record.delimiter","</features>");
				conf2.set("totalDocuments",""+totalDocuments);

				Job job2 = Job.getInstance(conf2,"Doc-Feature Matrix Job");
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

				job2.setOutputKeyClass(StockKey.class);
				job2.setOutputValueClass(Text.class);

				MultipleOutputs.addNamedOutput(job2, "Text", TextOutputFormat.class, Text.class, DoubleWritable.class);
				MultipleOutputs.addNamedOutput(job2, "Seq",SequenceFileOutputFormat.class,Text.class, DoubleWritable.class);
				FileOutputFormat.setOutputPath(job2, outputPath2);

				jobsSuccess = job2.waitForCompletion(true);
//	*/ //	
				}																			
			/*
			 * Job 3 Alpha: Read from Seqeunce File and do a Cross-Validation on splits of the matrix
			 */

			Configuration conf3 = new Configuration();
			totalRecords = 4378;
			splitsNum = (int)totalRecords/50;
			//conf3.setInt(NLineInputFormat.LINES_PER_MAP, splitsNum);
			conf3.setInt("splitsNum",splitsNum);

			Job job3 = Job.getInstance(conf3,"Nth Split Cross Validation"); 
			job3.setJarByClass(HBaseRunner.class);
			job3.setJobName("Nth Split Cross Validation");
			job3.setInputFormatClass(SequenceFileInputFormat.class);
			job3.setMapperClass(CrossValMapper.class);
			job3.setNumReduceTasks(0);
			SequenceFileInputFormat.addInputPath(job3, crossValPath);

			FileOutputFormat.setOutputPath(job3, outputPath3);


			if(jobsSuccess)
				System.out.println("Number of Documents: "+totalDocuments+
						"\nNumber of Features: "+totalFeatures+
						"\nNumber of Records: "+totalRecords);

			System.exit(jobsSuccess ? 0 : 1);

		}
		catch (Exception e){
			System.out.println("EXCEPTION CAUGHT RED-HANDED: ");
			e.printStackTrace();	
		}
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

	public static class TD_IDF_Reducer extends Reducer<StockKey,Text,Text,DoubleWritable>{
		MultipleOutputs<Text, DoubleWritable> mos;


		public void setup(Context context) {
			mos = new MultipleOutputs(context);
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}

		public void reduce(StockKey key, Iterable<Text> valueList, Context context)throws IOException , InterruptedException{
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
						System.out.println("Error in Reducer: IDF Key not found for feature: "+key.getSymbol());
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
					System.out.println("DocID: "+docID+" FeatureIndex: "+featureIndex+" Feature Name: "+key.getSymbol()+" TF_IDF: "+TF_IDF);
					mos.write("Text",new Text(docID+"\t"+key.getSymbol()),new DoubleWritable(TF_IDF));
					mos.write("Seq",new Text(docID+"\t"+key.getSymbol()),new DoubleWritable(TF_IDF));
					recordCount++;

				}
				count++;

			}
			totalRecords += recordCount;
			totalFeatures = featureCount;
		}

	}

	
	public static class CrossValMapper extends Mapper<Text, Text, Text, Text>{

		public enum Counters { LINES }

		public void map(Text DocID, Text line, Context context) throws IOException {
			
			 TaskAttemptID tid = context.getTaskAttemptID();
		      
		        // Set up the weka configuration
		        Configuration conf = context.getConfiguration();
		        int numMaps = Integer.parseInt(conf.get("splitsNum"));
		      //  classname = wekaConfig.get("Run.classify");
		      
		        String[] splitter = tid.toString().split("_");
		        String jobNumber = "";
		        int n = 0;
		      
		        if (splitter[4].length() > 0) {
		    	    jobNumber = splitter[4].substring(splitter[4].length() - 1);
		    	    n = Integer.parseInt(jobNumber);
		        }
		        
		        if( n ==1)
			System.out.println(DocID.toString());

		}
	}

}

