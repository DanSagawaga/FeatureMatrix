import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.io.IntWritable;
import java.util.*;




public class Job4_Mapper extends Mapper<IntWritable, Text, IntWritable, Text>{

    public enum Job4_Mapper_Counter { LINES }
    public static long totalDocuments = 0;
    public static int[] documentPartitions = new int[10];
    Random randomGenerator = new Random();

	public void setup(Context context) {
		System.out.println("\n******** Processing Job 4 Mapper ********\n");

		Configuration conf = context.getConfiguration();
		totalDocuments = Long.parseLong(conf.get("totalDocuments"));
		
		for(int k = 0; k < documentPartitions.length; k++){
			documentPartitions[k] = (int)((k+1) * totalDocuments/10);
		//	System.out.println(documentPartitions[k]);
		}
	}

	public void map(IntWritable DocID, Text line, Context context) throws IOException, InterruptedException {
		context.getCounter(Job4_Mapper_Counter.LINES).increment(1);//increments the counter so it can be used as indexer
		long docCounter = context.getCounter(Job4_Mapper_Counter.LINES).getValue();
		
/*
		for(int k = 0; k < documentPartitions.length; k++){		
			if(docCounter <= documentPartitions[k]){
				//System.out.println("Document#: " + docCounter+ " Partition Key: " + k + " " +DocID.toString());
				context.write(new IntWritable(k), new Text(DocID + line.toString()));
				break;
			}
		}
*/
		
		
//		System.out.println("Document#: " + docCounter+ " " + randomGenerator.nextInt((int)totalDocuments/10));
		context.write(new IntWritable(randomGenerator.nextInt((int)totalDocuments/10)), new Text(DocID + line.toString())); 
	}
}
/*
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
 */