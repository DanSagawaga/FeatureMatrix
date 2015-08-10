import java.io.IOException;
import java.io.StringReader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.input.SAXBuilder;
import java.util.*;



public class Job2_FeatureSet_Mapper extends Mapper<Text, Text, CompositeKey, Text> {  // co ImportFromFile-2-Mapper Define the mapper class, extending the provided Hadoop class.

	private static HashMap<String,Double> weightMap = new HashMap<String,Double>();
	
	public enum Counters { LINES }

	
    public void map(Text DocID, Text line, Context context) throws IOException {
    	
    	String xmlString = line.toString();
    	double tempWeight = 0, maxWeight = 0;
    	String TF = "";

    	
    	
		try {
			Document doc = new SAXBuilder().build(new StringReader(xmlString));
			if (doc!=null) {
				for (Element child : doc.getRootElement().getChildren()) {
					if (child.getName().equalsIgnoreCase("feature")) {
						tempWeight = Double.parseDouble(child.getAttribute("weight").getValue());
						if(tempWeight > maxWeight)
							maxWeight = tempWeight;
						
						weightMap.put(child.getAttribute("name").getValue(),tempWeight);
					}
				}
			}
     			
		    for (Map.Entry<String, Double> entry : weightMap.entrySet()) {
		        String feature = entry.getKey();
		        tempWeight = entry.getValue();
		        /*
		         * The Term frequency is calculated using the the equation tf(t,d) = .5 + (.5 * f(t,d) ) / maxWeight f(t,d)
		         */
		        TF ="" +(.5 + (.5 *tempWeight)/maxWeight);
		    //    System.out.println("Mapper Doc ID: " + DocID + " Feature: " + feature + " Term Freq: " + tempWeight/maxWeight);	        
			    context.write(new CompositeKey(feature, Double.parseDouble(DocID.toString())), new Text(DocID.toString() + "\t" + TF));	
			} 
		    weightMap.clear();

		}catch(Exception e){
			System.out.println("Error in the Mapper: "+e.getMessage());
		} 	
    }
    
    
}

