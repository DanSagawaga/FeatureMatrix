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
	static long docCount = 0;

	public static void main(String[] args) {
		try{
			if (args.length != 2) {
				System.out.printf(
						"\nNOT RIGHT AMOUNT OF ARGUMENTS\n");
				System.exit(-1);
			}
				
			Path docFreqPath = new Path(args[0]);//home/cloudera/Desktop/2-100/feature-sets/ne_all/docfreqs/part-00000			
			Path featureSetPath = new Path(args[1]);//home/cloudera/Desktop/2-100/feature-sets/ne_all/docfeaturesets-weighted/part-00000
			Path outputPath = new Path("/Users/dansaganome/Desktop/TF_IDF");

			/*
			 * Job 1
			 */
			Configuration conf1 = new Configuration();
			conf1.set("te"
					+ "xtinputformat.record.delimiter","</features>");

			Job job1 = Job.getInstance(conf1,"HBaseRunner");
			job1.setJarByClass(HBaseRunner.class);
			job1.setJobName("HBaseRunner Job");

			/*			job.setPartitionerClass(NaturalKeyPartitioner.class);
	        job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
	        job.setSortComparatorClass(CompositeKeyComparator.class);

			job.setMapperClass(HbaseDocFreqMapper.class);
			job.setMapperClass(HBaseMapperFeatureSet.class);
			job.setReducerClass(TD_IDF_Reducer.class);

			//job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setOutputKeyClass(StockKey.class);
            job.setOutputValueClass(Text.class);
            
            //     MultipleInputs.addInputPath(job, docFreqPath, SequenceFileInputFormat.class, HbaseDocFreqMapper.class);
			//     MultipleInputs.addInputPath(job,featureSetPath, SequenceFileInputFormat.class, HBaseMapperFeatureSet.class); 
			 */		
			SequenceFileInputFormat.addInputPath(job1, featureSetPath);
			job1.setMapperClass(DocCounterMapper.class);
			job1.setMapperClass(DocCounterMapper.class);
			job1.setInputFormatClass(SequenceFileInputFormat.class);
			job1.setReducerClass(DocCounterReducer.class);
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(IntWritable.class);
			job1.setOutputFormatClass(TextOutputFormat.class); 
			FileOutputFormat.setOutputPath(job1, null);

			boolean success = job1.waitForCompletion(true);
			if(success)
				System.out.println("Doc Number: "+docCount);

			System.exit(success ? 0 : 1);

		}
		catch (Exception e){
			System.out.println("EXCEPTION CAUGHT HAHAHAHA: " + e.getMessage());	
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
			docCount += sum;
		}
	}

}
