import java.io.*;
import java.util.*;




public class Driver {

	static float numOfFeatures = 0;
	static Number weightFeature;
	static float totalRawFeaturesInDocs = 0;
	static Number totalDocsWithFeature = 0;
	static Number totalDocs= 0;
	static float idf = 0;
	static String feature = "";
	
	static int tempDocID = 0;

	static HashMap<String, String> featIndex = new HashMap<String, String>(1000,990);


	public static void main (String args[]) throws IOException{
		
		/*
		 * PHASE 1 read 3 sequence files to get TotalDocs, TotalRawFreqs, TotalDocFreq, and index features 
		 */
		totalDocs = (float)getTotalDocNum("/Users/dansaganome/Desktop/2-100/class-memberships/ClassMemberships.txt");
		System.out.println(totalDocs);
		
		
		

		BufferedReader featReader = readDataFile("/Users/dansaganome/Desktop/2-100/feature-sets/ne_all/docfeaturesets-weighted/docfeaturesets-weighted.xml");
		
		Scanner scanner;
		String curLine = "", fullXml = "", partialXml = "";
		FeatureSetWeighted XmlReader = new FeatureSetWeighted(); 
		
		
		
		for(int k = 0; ((curLine = featReader.readLine())!= null); k++){
			scanner = new Scanner(curLine);
			
			if(scanner.hasNextInt()){		//if the line has an int, then it is parsed as Doc ID and the rest is appened to the xml string
				tempDocID = scanner.nextInt();
				partialXml = scanner.next() + "\n";
			}
			else if(!curLine.equals("</features>")){	//The rest of the lines as appened to the xml string
				partialXml = partialXml + curLine + "\n";
			}
			else if(curLine.equals("</features>")){	//once the full xml string is set it is ready to process
				fullXml = partialXml + curLine;
			//	System.out.println(fullXml);
				if(fullXml != null){
				XmlReader = new FeatureSetWeighted(fullXml);
				}
		//		System.out.println(fullXml);
			}
			XmlReader.getWeight();
			
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

	public static void indexFeaturesWithTF(String filePath){
		
		BufferedReader featReader = readDataFile(filePath);

		String curLine = "";
		StringTokenizer tokenizer = null;
		
		try{
		for(int k = 0; ((curLine = featReader.readLine())!= null); k++){
			/*
			 * Reads the Doc Frequency file first and the Doc frequency 
			 */
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
			 * Computes the IDF Line values and stores it in the HashMap Table
			 * Hash input <featureName, IDF> --k will be the feature index or column index
			 */
			//
            	idf = (float) Math.log10(totalDocs.floatValue() / (Float.MIN_VALUE + totalDocsWithFeature.floatValue()));
            	String idfWithIndex = (k + " " + idf) ;
            	
            	featIndex.put(feature,idfWithIndex);			
		}
		}catch(Exception e){
			System.out.println("ERROR: Reading in totalDocs files: " + e.getMessage());
		}
		numOfFeatures = featIndex.size();
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
