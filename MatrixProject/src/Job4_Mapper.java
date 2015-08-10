import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import java.util.Random; 


public class Job4_Mapper extends Mapper<Text, Text, Text, Text>{

	int mapperNum = 0, randomSeed = 0;
	long docCounter = 0;

	Random rand = new Random();
	enum Job4_Mapper_Counter { LINES }


	public void setup(Context context) {
		TaskAttemptID tid = context.getTaskAttemptID();		
		String[] splitter = tid.toString().split("_");
		mapperNum = Integer.parseInt(splitter[4]);

		System.out.println("\n****************** Processing Job 4 Mapper: "+mapperNum+ " ******************\n");

		Configuration conf = context.getConfiguration();
		randomSeed = Integer.parseInt(conf.get("randomSeed"));

	}

	public void map(Text docID_Class_Text, Text feature_Set, Context context) throws IOException, InterruptedException {
		docCounter = context.getCounter(Job4_Mapper_Counter.LINES).getValue();
		
		if(randomSeed < 2)
			context.write(new Text(""+docCounter), new Text(docID_Class_Text.toString()+"\n"+feature_Set.toString()));
		else
			context.write(new Text(""+rand.nextInt(randomSeed)), new Text(docID_Class_Text.toString()+"\n"+feature_Set.toString()));

		context.getCounter(Job4_Mapper_Counter.LINES).increment(1);//increments the counter so it can be used as indexer

	}
	protected void cleanup(Context context) throws IOException, InterruptedException {

	}
}
