import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public  class DocCounterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	int totalDocuments = 0;

	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : values) 
			sum += val.get();
	//	System.out.println("Doc Counter Reduecer Counter++");
		totalDocuments += sum;
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		context.write(new Text(""), new IntWritable(totalDocuments));

	}

}