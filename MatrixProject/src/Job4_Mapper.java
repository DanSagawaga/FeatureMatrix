import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.input.SAXBuilder;
import org.apache.hadoop.io.WritableComparable;


import java.util.*;

public class Job4_Mapper extends Mapper<Text, Text, IntWritable, Text>{

	static 	Random randomGenerator = new Random();
	public enum Job4_Mapper_Counter { LINES }
	public int totalDocuments = 0, numDocsInFold = 0,  currentPartition = 0;


	public void setup(Context context) {
		System.out.println("\n******** Processing Job 4 Mapper ********\n");
		Configuration conf = context.getConfiguration();
		totalDocuments = Integer.parseInt(conf.get("totalDocuments"));
		numDocsInFold = (int)(totalDocuments/10);
	}

	public void map(Text docID_Classifier_Text, Text feature_Set, Context context) throws IOException, InterruptedException {
		context.getCounter(Job4_Mapper_Counter.LINES).increment(1);//increments the counter so it can be used as indexer
		long docCounter = context.getCounter(Job4_Mapper_Counter.LINES).getValue();

		if(docCounter%(numDocsInFold +1) != 0){
			//System.out.println("Document#: " + docCounter+ " Partition Key: " + k + " " +DocID.toString());
			context.write(new IntWritable(currentPartition), new Text(docID_Classifier_Text.toString() + "\n"+feature_Set.toString()));
	//		System.out.println(currentPartition+"\t"+docID_Classifier_Text.toString() );//+ "\n"+feature_Set.toString());
		}
		else if(currentPartition != 10){
			context.write(new IntWritable(currentPartition), new Text(docID_Classifier_Text.toString() + "\n"+feature_Set.toString()));
		//	System.out.println(currentPartition+"\t"+docID_Classifier_Text.toString() );//+ "\n"+feature_Set.toString());
			currentPartition++;
		}


		//	context.write(new IntWritable(randomGenerator.nextInt(10)), new Text(docID_Classifier_Text.toString() + "\t"+feature_Set.toString())); 

		//System.out.println(Integer.parseInt(DocID_Feat_Str[0])+"\t"+(DocID_Feat_Str[1]+"\t"+ TFxIDF.toString()));
	}
}
