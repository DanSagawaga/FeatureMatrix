import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.util.*;


public class TD_IDF_Reducer  extends Reducer<StockKey,Text,Text,DoubleWritable>
{
	public void reduce(StockKey key, Iterable<Text> valueList, Context context)
			throws IOException , InterruptedException
	{
		String IDF_Str = null;
		String TF_IDF_Matrix_Record = null;
		String docID = null;
		double IDF = 0.0, TF = 0.0,TF_IDF = 0.0;


		Scanner scan = null;

	//	if(key.getSymbol().equals("11")){

			int count = 0;	
			for(Text value : valueList){
				if(count == 0){
					scan = new Scanner(value.toString());
					scan.useDelimiter("\t");
					if(scan.next().equals("IDF_Flag")){
						IDF = Double.parseDouble(scan.next());
					//	System.out.println("IDF: "+ IDF);
					}
					else{
						System.out.println("Error in Reducer: IDF Key not found for feature: "+key.getSymbol());
						break;
					}	
				}
				else{
					scan = new Scanner(value.toString());
					scan.useDelimiter("\t");
					docID = scan.next();
					TF = Double.parseDouble(scan.next());
					TF_IDF = TF * IDF;
					System.out.println("DocID: "+docID+" Feature: "+key.getSymbol()+" TF: "+TF+" IDF: "+IDF+" TF_IDF: "+TF_IDF);
					context.write(new Text(docID+"\t"+key.getSymbol()),new DoubleWritable(TF_IDF));
					
				}
				count++;
		//	}
		}
	}
}
