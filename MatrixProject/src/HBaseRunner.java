import java.io.IOException;

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



public class HBaseRunner  {
	static long totalDocuments = 0;
	static boolean jobsSuccess = false;

	public static void main(String[] args) {
		try{
			if (args.length != 2) {
				System.out.printf(
						"\nNOT RIGHT AMOUNT OF ARGUMENTS\n");
				System.exit(-1);
			}

			Path docFreqPath = new Path(args[0]);//home/cloudera/Desktop/2-100/feature-sets/ne_all/docfreqs/part-00000			
			Path featureSetPath = new Path(args[1]);//home/cloudera/Desktop/2-100/feature-sets/ne_all/docfeaturesets-weighted/part-00000
			Path outputPath = new Path("/home/cloudera/Documents/TF_IDF");
			Path outputPath2 = new Path("/home/cloudera/Documents/TF_IDF2");

			/*
			 * Job 1 Goes through the DocFeatureSet sequence file to count the number of Documents
			 * to pass to the next job
			 */
			Configuration conf = new Configuration();
			conf.set("xtinputformat.record.delimiter","</features>");
			conf.set("totalDocuments","Dan is Great");


			Job job1 = Job.getInstance(conf,"Document Counter Job");
			job1.setJarByClass(HBaseRunner.class);
			job1.setJobName("Document Counter Job");

			SequenceFileInputFormat.addInputPath(job1, featureSetPath);
			job1.setMapperClass(DocCounterMapper.class);
			job1.setMapperClass(DocCounterMapper.class);
			job1.setInputFormatClass(SequenceFileInputFormat.class);
			job1.setReducerClass(DocCounterReducer.class);
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(IntWritable.class);
			job1.setOutputFormatClass(TextOutputFormat.class); 
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
				FileOutputFormat.setOutputPath(job2, outputPath2);

				jobsSuccess = job2.waitForCompletion(true);
			}


			if(jobsSuccess)
				System.out.println("Doc Number: "+totalDocuments);

			System.exit(jobsSuccess ? 0 : 1);

		}
		catch (Exception e){
			System.out.println("EXCEPTION CAUGHT RED-HANDED: " + e.getMessage());	
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

}
