import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configuration;



public class Job3_Reducer extends Reducer<IntWritable,Text,IntWritable,Text>{
	MultipleOutputs<Text, DoubleWritable> mos;




	public void setup(Context context) {
		System.out.println("\n******** Processing Job 3 Reducer ********\n");
		mos = new MultipleOutputs(context);
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
	}



	public void reduce(IntWritable DocID, Iterable<Text> values, Context context)throws IOException , InterruptedException{
		
		String featureList = "";
		for(Text value: values){
			featureList +=  value.toString() + "\n";
		}
		context.write(new IntWritable(DocID.get()), new Text(featureList));
		//System.out.println("DocID: " +DocID.toString()+"\n"+ featureList);
    //	context.getCounter(Job3_Reducer_Counter.LINES).increment(1);//increments the counter so it can be used as indexer
	//	System.out.println(context.getCounter(Job3_Reducer_Counter.LINES).getValue());

	//	if(context.getCounter(Job3_Reducer_Counter.LINES).getValue() < totalDocuments/10)
	//		System.out.println("First Document: " +context.getCounter(Job3_Reducer_Counter.LINES).getValue());
		
	}
}