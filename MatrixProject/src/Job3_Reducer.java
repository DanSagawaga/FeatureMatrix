import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Job3_Reducer extends Reducer<CompositeKey,Text,Text,Text>{

	public void setup(Context context) {
		System.out.println("\n******** Processing Job 3 Reducer ********\n");
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
	}



	public void reduce(CompositeKey DocID, Iterable<Text> values, Context context)throws IOException , InterruptedException{
		String classifier = "", featureList = "";
		int count = 0;
		for(Text value: values){
			if(count == 0){	//the first line holds the classifier
				classifier = value.toString();
				count++;
			}
			else
			featureList +=  value.toString() + "\n";
		}
		
	//	System.out.println(DocID.getPrimaryKey() + " " + classifier);
	
		if(featureList.toString().equals(""))
			System.out.println("Document: "+ DocID.getPrimaryKey()+ " has 0 features | Has been ommited from matrix.");
		else 
		context.write(new Text(DocID.getPrimaryKey()+"\t"+classifier), new Text(featureList));
		
		
		//System.out.println("DocID: " +DocID.toString()+"\n"+ featureList);
    //	context.getCounter(Job3_Reducer_Counter.LINES).increment(1);//increments the counter so it can be used as indexer
	//	System.out.println(context.getCounter(Job3_Reducer_Counter.LINES).getValue());

	//	if(context.getCounter(Job3_Reducer_Counter.LINES).getValue() < totalDocuments/10)
	//		System.out.println("First Document: " +context.getCounter(Job3_Reducer_Counter.LINES).getValue());
		
	}
}