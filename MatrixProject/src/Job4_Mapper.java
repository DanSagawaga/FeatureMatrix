import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.io.IntWritable;
import java.util.*;




public class Job4_Mapper extends Mapper<Text, Text, IntWritable, Text>{

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

	public void map(Text docID_Classifier_Text, Text feature_Set, Context context) throws IOException, InterruptedException {
		context.getCounter(Job4_Mapper_Counter.LINES).increment(1);//increments the counter so it can be used as indexer
		long docCounter = context.getCounter(Job4_Mapper_Counter.LINES).getValue();
		
		String[] docID_Classifier_Str = docID_Classifier_Text.toString().split("\t");
		
	//	System.out.println(docID_Classifier_Str[0] + "\t"+ docID_Classifier_Str[1]);
		
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
		context.write(new IntWritable(randomGenerator.nextInt(10)), new Text(docID_Classifier_Str[0] + "\t"+ docID_Classifier_Str[1]+"\n"+feature_Set.toString())); 
	}
}
