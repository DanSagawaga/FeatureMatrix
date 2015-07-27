import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Job3_Mapper_2  extends Mapper<Text, Text, CompositeKey, Text> {

	public void setup(Context context) {
		System.out.println("\n******** Processing Job 3 Mapper 2 ********\n");
	}

	public void map(Text DocID, Text classMembership, Context context) throws IOException, InterruptedException{ // co ImportFromFile-3-Map The map() function transforms the key/value provided by the InputFormat to what is needed by the OutputFormat.

		
		//System.out.println("DocID: " + DocID.toString() + " Class: " + classMembership.toString());
		context.write(new CompositeKey(DocID.toString(),-1.0), new Text(classMembership.toString()));
	}
}

