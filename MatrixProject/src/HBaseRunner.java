import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.mapred.SequenceFileAsTextInputFormat;


public class HBaseRunner  {
	public static void main(String[] args) {
		try{
			if (args.length != 1) {
				System.out.printf(
						"\nNOT RIGHT AMOUNT OF ARGUMENTS\n");
				System.exit(-1);
			}

			Configuration conf = HBaseConfiguration.create();
			conf.set("textinputformat.record.delimiter","</features>");
			//admin.pickTable("DanTestTable");
			//admin.putRecord("IndexRowTest1","FeatureFamily", "EmptyFeature", 0);

			// Configuration conf = admin.getConfiguration();


			Job job = Job.getInstance(conf,"HBaseRunner");
			
			job.setInputFormatClass(SequenceFileInputFormat.class);
			
			
			job.setJarByClass(HBaseRunner.class);
			job.setJobName("HBaseRunner Job");
			
	/*	
			
			job.setMapperClass(HbaseDocFreqMapper.class);
	//		job.setMapperClass(HBaseMapperFeatureSet.class);
			job.setOutputFormatClass(TableOutputFormat.class);
			job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "DanTestTable");
			job.setNumReduceTasks(0); 

			FileInputFormat.setInputPaths(job, new Path(args[0]));   

	*/	
			
//	/*	
			Scan scan = new Scan();
			scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
			scan.setCacheBlocks(false);  //
			
			TableMapReduceUtil.initTableMapperJob(
				"DanTestTable",        // input table
				scan,               // Scan instance to control CF and attribute selection
				TFIDF_Mapper.class,     // mapper class
				null,         // mapper output key
				null,  // mapper output value
				job);
			//job.setOutputFormatClass(NullOutputFormat.class);   // because we aren't emitting anything from mapper

			
			TableMapReduceUtil.initTableReducerJob(
				"DanTestTable2",        // output table
				null,    // reducer class
				job);
			job.setNumReduceTasks(0); 

//			*/
			







			boolean success = job.waitForCompletion(true);
			System.exit(success ? 0 : 1);

		}
		catch (Exception e){
			System.out.println("EXCEPTION CAUGHT HAHAHAHA: " + e.getMessage());	
		}

	}
}
