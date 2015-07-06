import java.io.*;
import java.util.*;

import org.jdom2.*;
import org.jdom2.input.SAXBuilder;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;




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
	
	static HashMap<String, String> DocFreqHash = new HashMap<String, String>(1000,990); // <String feature, String index + docFreq>
	static HashMap<String, Float> FeatureSetWeightedHash = new HashMap<String, Float>(1000,990); // <String Doc ID + feature Index, weight>
	static HashMap<Integer, Float> MaxFeatureWeight = new HashMap<Integer, Float>(1000,990); // <String Doc ID + feature Index, weight>



	public static void main (String args[]) throws IOException{

		/*
		 * PHASE 1 read 3 sequence files to get TotalDocs, TotalRawFreqs, TotalDocFreq, and index features 
		 */
		//totalDocs = (float)getTotalDocNum("/Users/dansaganome/Desktop/2-100/class-memberships/ClassMemberships.txt");
	//	System.out.println(totalDocs);

		ReadDocFreq("/home/cloudera/Desktop/2-100/feature-sets/ne_all/docfreqs/DocFreqs.txt");
		
		BufferedReader featReader = readDataFile("/home/cloudera/Desktop/2-100/feature-sets/ne_all/docfeaturesets-weighted/docfeaturesets-weighted.xml");
		
		Scanner scanner;
		StringTokenizer tokenizer; 
		SAXBuilder saxBuilder = new SAXBuilder();
		String curLine = "";
		float maxWeight = 0;

		
		for(int k = 0; ((curLine = featReader.readLine())!= null) && k < 18 ; k++){
			scanner = new Scanner(curLine);
			
			if(scanner.hasNextInt()){		//if the line has an int, then it is parsed as Doc ID and the rest is appened to the xml string
				tempDocID = scanner.nextInt();
				maxWeight = 0;
				totalDocs++;
			}
			else if(!curLine.equals("</features>") && !curLine.equals("")){	
				
				try {
					curLine = "<features>" + curLine + "<features/>";
				    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			         DocumentBuilder builder = factory.newDocumentBuilder();
			         Document doc = builder.parse(curLine);


					
					
				    System.out.println(doc.getAttribute(FeatureSet.XML_TAG_FEATURE_NAME).getValue());
				} catch (JDOMException e) {
				    // handle JDOMException\
				}
				
				tokenizer = new StringTokenizer(curLine,"\"");
				tokenizer.nextToken();
				feature = tokenizer.nextToken();							//Gets Feature Name
				tokenizer.nextToken();
				tempFeatureWeight = Float.parseFloat(tokenizer.nextToken());//Gets Feature Weight
				if(tempFeatureWeight > maxWeight)
					maxWeight = tempFeatureWeight;
				
				/*
				 * Hashes values into <String Doc ID + feature Index, weight>
				 */
				
			//	scanner = new Scanner(DocFreqHash.get(feature));
				
			//	System.out.println(feature +"  " + DocFreqHash.get(feature));
			//	int temp = scanner.nextInt();
			//	FeatureSetWeightedHash.put((tempDocID + " " + temp), tempFeatureWeight);
			}
			else if(curLine.equals("</features>")){
			}
			/*
			 * Hashes the Document's feature' max weight;
			 */
				MaxFeatureWeight.put(tempDocID,maxWeight);
			}
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

	public static void ReadDocFreq(String filePath){
		/*
		 * Reads the Doc Frequency file first and the Doc frequency 
		 * Puts values into DocFreq Hash
		 *  HashMap <feature, (index + doqFreq)>
		 */
		
		BufferedReader featReader = readDataFile(filePath);
		String curLine = "";
		StringTokenizer tokenizer = null;
		
		
		try{
		for(int k = 0; (((curLine = featReader.readLine())!= null) && (!curLine.equals(""))); k++){

			tokenizer = new StringTokenizer(curLine,"\t");
			if(tokenizer.hasMoreTokens())
			feature = tokenizer.nextToken();
			else{
				System.out.println("DocFreq Line " + k + " : bad feature input, replaced with a \" \" instead");
				feature = "";
			}
			if(tokenizer.hasMoreTokens())
				totalDocsWithFeature = Integer.parseInt(tokenizer.nextToken());
			else{
				System.out.println("Line " + k + " : bad number input, replaced with a 0 instead");
				totalDocsWithFeature = 0;
			}
			//System.out.println(k + ": " + feature + "\t" + totalDocsByFeat + "\n");
			/*
			 * Hashes the values into a <String feature, String Feature Index + DocFreq> 
			 */
			//System.out.println(k +" " + feature + " "  + " " + totalDocsWithFeature);
			DocFreqHash.put(feature,(k + " " + totalDocsWithFeature));
		}
		}catch(Exception e){
			System.out.println("ERROR: Reading in totalDocs files: " + e.getMessage());
		}
		numOfFeatures = DocFreqHash.size();
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





/*
 * 
 * File file = new File("/home/cloudera/Desktop/2-100/feature-sets/ne_all/dictionary/part-00000.txt");
	file.getParentFile().mkdirs();
	
	PrintWriter writer = new PrintWriter(file);
	
	writer.println("The first line");
	writer.println("The second line");
	writer.close();
 * 
 * 
 * 
 * 
 */
