import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



/*
 * Job 3 Mapper 2 / Class Membership Mapper
 * 
 * This mapper simply reads the Class Membership sequence file to assign the class to each document in the reducer.
 * Secondary sorting in this job is used primarily for the purpose of sorting the feature indices, 
 * but it also serves the purpose of allowing the class membership to come first. This makes the formating in the Reducer simpler 
 * 
 */
public class Job3_Mapper_2  extends Mapper<Text, Text, CompositeKey, Text> {

	public void setup(Context context) {
		System.out.println("\n******** Processing Job 3 Mapper 2 ********\n");
	}

	public void map(Text DocID, Text classMembership, Context context) throws IOException, InterruptedException{ // co ImportFromFile-3-Map The map() function transforms the key/value provided by the InputFormat to what is needed by the OutputFormat.

		//System.out.println("DocID: " + DocID.toString() + " Class: " + classMembership.toString());
		context.write(new CompositeKey(DocID.toString(),-1.0), new Text(classMembership.toString()));
	}
}

