import java.io.IOException;
import java.io.StringReader;

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


/*
 * Job 3 Mapper / Doc ID, Feature, TFxIDF Mapper
 * 
 * A compositeKey object is used again to have secondary sorting.
 * This is done for 2 reasons. Primarily to always have the output of the other mapper come first
 * so that in the reducer the first iterable will be the class membership in order to to concatinate it with 
 * the rest of the Document iterables.
 * Secondly, since secondary sorting sorts the values, by using the feature index value as the secondary key,
 * the feature indices and their respective TF_IDF value will be sorted in ascending order.
 * This sorting is required by Weka's classifiers when training and testing on sparse vectors. 
 * 
 */

public class Job3_Mapper extends Mapper<Text, DoubleWritable, CompositeKey, Text> {

	public void setup(Context context) {
		System.out.println("\n******** Processing Job3_Mapper ********\n");
	}

    public void map(Text DocID_FeatIndex_Text, DoubleWritable TFxIDF, Context context) throws IOException, InterruptedException{ // co ImportFromFile-3-Map The map() function transforms the key/value provided by the InputFormat to what is needed by the OutputFormat.
    	
    	String[] DocID_FeatIndex_Str = DocID_FeatIndex_Text.toString().split("\t");  
    	String DocID = DocID_FeatIndex_Str[0], FeatureIndex = DocID_FeatIndex_Str[1];

    	context.write(new CompositeKey(DocID_FeatIndex_Str[0],Double.parseDouble(FeatureIndex)),
    			new Text(FeatureIndex+"\t" +TFxIDF.toString()));
    	//System.out.println(Integer.parseInt(DocID_Feat_Str[0])+"\t"+(DocID_Feat_Str[1]+"\t"+ TFxIDF.toString()));
    }
}
