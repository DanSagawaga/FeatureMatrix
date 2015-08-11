import java.io.IOException;
import java.io.StringReader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.input.SAXBuilder;
import java.util.*;

/*
 * Job 2 FeatureSet Mapper
 * 
 * This Mapper reads from the Feature Set Sequence File containing all the documents ID's with their set of features
 * and the weights of those features in that document. The data is processed from xml format.
 * A composite object is used as the key. The features are the primary key and Doc ID is used as the secondary key.
 * Having the feature as the primary key allows for the grouping of this mapper's Term frequency value with that of the DocFreq mapper
 * The Document ID is used as the secondary key for secondary sorting. Assuming that no dcoument ID will be be less than 0,
 * all the values written from this mapper will come after the one DocFreq file in the reducer. This allows all the term frequency 
 * values written from here to be multiplied with the one inverse document frequency value to get the TFxIDF matrix. 
 * 
 */

public class Job2_FeatureSet_Mapper extends Mapper<Text, Text, CompositeKey, Text> {  

	//Map holds all the weights of the features in each document in order to get the largest weight in order to normalize the weights 
	private static HashMap<String,Double> weightMap = new HashMap<String,Double>();

	public void map(Text DocID, Text line, Context context) throws IOException {

		String xmlString = line.toString();
		double tempWeight = 0, maxWeight = 0;
		String TF = "";

		/*
		 * Reads in the xml string and extracts the document, set of features, their weights, and the the max weight
		 * 
		 */
		try {
			Document doc = new SAXBuilder().build(new StringReader(xmlString));
			if (doc!=null) {
				for (Element child : doc.getRootElement().getChildren()) {
					if (child.getName().equalsIgnoreCase("feature")) {
						tempWeight = Double.parseDouble(child.getAttribute("weight").getValue());
						if(tempWeight > maxWeight)
							maxWeight = tempWeight;
						//weights are placed in HashMap
						weightMap.put(child.getAttribute("name").getValue(),tempWeight);
					}
				}
			}

			/*
			 * For each feature a record is written containing it's document ID and its term frequency value
			 */
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

