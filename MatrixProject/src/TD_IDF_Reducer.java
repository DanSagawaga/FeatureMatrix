import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.util.*;


public class TD_IDF_Reducer  extends Reducer<Text,Text,Text,Text>
{
    public void reduce(Text key, Iterable<Text> valueList, Context con)
     throws IOException , InterruptedException
     {
    	String IDF_Str = null;
    	String TF_IDF_Matrix_Record = null;
    	String docID = null;
    	double IDF = 0.0, TF_IDF = 0.0;
    	Scanner scan = null;
    	ArrayList<String> valueArray = new ArrayList<String>();
    	
    			if(key.toString().equals("29 September , 1967")){
    				
    			for(Text value : valueList){
    				System.out.println(value.toString());//Loops to find the IDF value in the list of values
    				valueArray.add(value.toString());
    			}
				IDF_Str = valueArray.get(valueArray.size()-1);		//should be the last value in the list
				//System.out.println(IDF);

    				
    			scan = new Scanner(IDF_Str);
    			scan.useDelimiter(" ");
    			//Checks that the IDF value is the last value 
    			if(scan.next().equals("IDF_Flag"))
    				IDF = Double.parseDouble(scan.next());
    				//System.out.println(IDF);
    			

    			for(Text value : valueList){   
    				//System.out.println(valueArray.size());

    			//	System.out.println(value.toString());
    				scan = new Scanner(value.toString());
    				docID = scan.next();
    				if(!docID.equals("IDF_Flag"))
    					TF_IDF = Double.parseDouble(scan.next()) * IDF;
    				
    				TF_IDF_Matrix_Record = docID +"\t" + key.toString() + TF_IDF;
    				
    				System.out.println(TF_IDF_Matrix_Record);		
    			}
    			

     }
}
}
