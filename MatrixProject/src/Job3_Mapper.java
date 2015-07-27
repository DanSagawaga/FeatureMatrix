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

public class Job3_Mapper extends Mapper<Text, DoubleWritable, CompositeKey, Text> {

	public void setup(Context context) {
		System.out.println("\n******** Processing Job3_Mapper ********\n");
	}
    
    public void map(Text DocID_Feat_Text, DoubleWritable TFxIDF, Context context) throws IOException, InterruptedException{ // co ImportFromFile-3-Map The map() function transforms the key/value provided by the InputFormat to what is needed by the OutputFormat.
    	
    	String[] DocID_Feat_Str = DocID_Feat_Text.toString().split("\t");
    	
    	context.write(new CompositeKey(DocID_Feat_Str[0],1.0), new Text(DocID_Feat_Str[1]+"\t"+ TFxIDF.toString()));
    	//System.out.println(Integer.parseInt(DocID_Feat_Str[0])+"\t"+(DocID_Feat_Str[1]+"\t"+ TFxIDF.toString()));
    }
}
