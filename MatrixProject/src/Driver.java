import java.io.*;
import java.util.*;
import java.lang.Math;
//import org.w3c.dom.Document;


public class Driver {

	static float numOfFeatures = 0;
	static float tempFeatureWeight;
	static float totalRawFeaturesInDocs = 0;
	static Number totalDocsWithFeature = 0;
	static int totalDocs= 0;
	static float idf = 0;
	static String feature = "";
	static float maxWeight = 0;
	static int tempDocID = 0;
	static HBaseManager HBaseAdmin = null;

	static HashMap<String, String> feature_Index_docFreq = new HashMap<String, String>(1000,990); // <String feature, String index + docFreq>
	static HashMap<Integer, String> FeatureSetWeightedHash = new HashMap<Integer, String>(); // <String Doc ID + feature Index, weight>
	static HashMap<Integer, Float> MaxFeatureWeight = new HashMap<Integer, Float>(1000,990); // <String Doc ID + feature Index, weight>



	public static void main (String args[]) throws Exception{

		HBaseAdmin= new HBaseManager();
		HBaseAdmin.pickTable("DanTestTable");
		//HBaseAdmin.deleteTable("DanTestTable");

	//	HBaseAdmin.createTable("DanTestTable","FeatureFamily");
		HBaseAdmin.listTables();
		
		//System.out.println(Arrays.toString(HBaseAdmin.getColumns("Index_Row","FeatureFamily")));
		System.out.println(HBaseAdmin.getRecord("IDF_Row", "FeatureFamily","Joel Furr"));
		
		//	public  String getRecord(String rowPar, String familyNamePar, String columnPar)throws IOException{
	//	System.out.println(HBaseAdmin.getFamilyNames());
		
	//	readDocFreq("/home/cloudera/Desktop/2-100/feature-sets/ne_all/docfreqs/DocFreqs.txt");
	//	readDocFeatureSetsWeighted("/home/cloudera/Desktop/2-100/feature-sets/ne_all/docfeaturesets-weighted/docfeaturesets-weighted.xml");
	//	String[] allFeatures = HBaseAdmin.getColumns("IndexRowTest1","FeatureFamily");

	//	System.out.println(HBaseAdmin.getRecord("IndexRowTest1","FeatureFamily",allFeatures[0]));
		
/*		for(int k = 0; k < allFeatures.length; k++){
			FeatureSetWeightedHash.put(Integer.parseInt(HBaseAdmin.getRecord("IndexRowTest1","FeatureFamily",allFeatures[k])), allFeatures[k]);
		}

		for(int k = 1; k < allFeatures.length; k++){
			System.out.println(k + " " + FeatureSetWeightedHash.get(k) + "\n");
		}
		
*/
	}

	public static int getTotalDocNum(String filePath){
		//Reads ClassMembership seq file to get total number of Documents
		BufferedReader featReader = readDataFile(filePath);
		int counter = 0;
		try{
			while((featReader.readLine()!= null)){
				counter++;
			}
			return counter;
		} catch(IOException e){
			System.err.println("Error counting total documents: " + e.getMessage());
			return -1;
		}
	}

	/*
	 * Reads the Doc Frequency file first and the Doc frequency 
	 * Puts values into DocFreq Hash
	 *  HashMap <feature, (index + doqFreq)>
	 */
	public static void readDocFreq(String filePath){

		BufferedReader featReader = readDataFile(filePath);
		String curLine = "";
		StringTokenizer tokenizer = null;


		try{
			for(int k = 0; (((curLine = featReader.readLine())!= null) && (!curLine.equals(" "))); k++){

				tokenizer = new StringTokenizer(curLine,"\t");
				if(tokenizer.hasMoreTokens())
					feature = tokenizer.nextToken();
				else{
					System.out.println("Bad DocFreq Feature " + (k+1) + " " + feature);
					feature = "";
				}
				if(tokenizer.hasMoreTokens())
					totalDocsWithFeature = Integer.parseInt(tokenizer.nextToken());
				else{
					System.out.println("Bad DocFreq Feeature: " + (k+ 1) + "\n" + feature);
					totalDocsWithFeature = 0;
				}
				//System.out.println(k + ": " + feature + "\t" + totalDocsByFeat + "\n");
				/*
				 * Hashes the values into a <String feature, String Feature Index + DocFreq> 
				 */
				//System.out.println(k +" " + feature + " "  + " " + totalDocsWithFeature);
				feature_Index_docFreq.put(feature,(k + "\t" + totalDocsWithFeature));
			}
		}catch(Exception e){
			System.out.println("ERROR: Reading in totalDocs files: " + e.getMessage());
		}
		numOfFeatures = feature_Index_docFreq.size();
	}
	/*
	 * Method reads in xml file line by line retrieving the the Document ID, it's features and their respective weights
	 * A matrix representation is made using the Doc ID, feature column Index, and weight as value 
	 * The total amount of Documents is calculated as well
	 * 
	 */
	public static void readDocFeatureSetsWeighted(String filePath) throws IOException{

		BufferedReader featReader = readDataFile(filePath);
		Scanner scanner;
		StringTokenizer tokenizer; 
		//FeatureSetWeighted Xmlparser = new FeatureSetWeighted();
		String curLine = "";
		float maxWeight = 0;

		for(int k = 0; ((curLine = featReader.readLine())!= null); k++){ //loop that runs through file

			scanner = new Scanner(curLine);	
			if(scanner.hasNextInt()){			//if the line has an integer then it is a Document ID and the rest is discarded
				tempDocID = scanner.nextInt();	//stoes Document ID in local integer
				maxWeight = 0;
				totalDocs++;					//counts the documents in file
			}
			else if(!curLine.equals("</features>") && !curLine.equals("")){	//any line that does not have the closing tag or empty is a feature line

				curLine = " <features>" + curLine +  "</features>"; //adds opening and closing tags to each line so they can be parsed with FeatureSetWeighted Class
			//	curLine = Xmlparser.parseXML(curLine); //calls FeatureWeight object to parse XML format to string consisting of the feature and weight 

				tokenizer = new StringTokenizer(curLine,"\t");
				feature = tokenizer.nextToken();							//Gets Feature Name
				tempFeatureWeight = Float.parseFloat(tokenizer.nextToken());//Gets Feature Weight

				System.out.println(feature + "\t" + tempFeatureWeight);
				if(tempFeatureWeight > maxWeight)
					maxWeight = tempFeatureWeight;

				/*
				 * Hashes values into <String Doc ID + feature Index, weight>
				 */

				scanner = new Scanner(feature_Index_docFreq.get(feature));	//retrieves the column index of the feature
		//		FeatureSetWeightedHash.put((tempDocID + " " + scanner.nextInt()), tempFeatureWeight); // places into matrix represenation of hash 
				//(tempDocID,"FeatureFamily",feature, tempFeatureWeight);
				
			//	HBaseAdmin.putRecord(tempDocID, "FeatureFamily", feature, tempFeatureWeight);
			}
			else if(curLine.equals("</features>")){
			}
			/*
			 * Hashes the Document's feature' max weight;
			 */
			MaxFeatureWeight.put(tempDocID,maxWeight);
		}		
	}

	public static BufferedReader readDataFile(String filename) {
		BufferedReader inputReader = null;
		try {
			inputReader = new BufferedReader(new FileReader(filename));
		} catch (FileNotFoundException ex) {
			System.err.println("File not found: " + filename);
		}

		return inputReader;
	}
}

