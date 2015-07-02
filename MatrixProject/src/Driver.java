import java.io.*;
import java.util.*;




public class Driver {

	static float numOfFeatures = 0;
	static float totalRawFeaturesInDocs = 0;
	static float totalDocsByFeat = 0;
	static float totalDocs= 0;
	static float idf = 0;
	static String feature = "";
	
	static int tempDocID = 0;

	static HashMap<Integer, Float> featIndex = new HashMap<Integer, Float>();


	public static void main (String args[]) throws IOException{
		
		totalDocs = (float)getTotalDocNum("/Users/dansaganome/Desktop/2-100/class-memberships/ClassMemberships.txt");
		System.out.println(totalDocs);
		
		BufferedReader featReader = readDataFile("/Users/dansaganome/Desktop/2-100/feature-sets/ne_all/docfreqs/DocFreqs.txt");
		BufferedReader featReader2 = readDataFile("/Users/dansaganome/Desktop/2-100/feature-sets/ne_all/rawfreqs/part-00000.txt");

		String curLine = "", curLine2 = "";
		Scanner scanner; 
		StringTokenizer tokenizer = null;
		
		// Reads dictionary to index features into a hash map
		for(int k = 0; ((curLine = featReader.readLine())!= null) && ((curLine2 = featReader2.readLine())!= null); k++){
			tokenizer = new StringTokenizer(curLine,"\t");
			if(tokenizer.hasMoreTokens())
			feature = tokenizer.nextToken();
			if(tokenizer.hasMoreTokens())
			totalDocsByFeat = Integer.parseInt(tokenizer.nextToken());
			
			
			tokenizer = new StringTokenizer(curLine2,"\t");	//gets the raw number of features in all documents
			if(tokenizer.hasMoreTokens())
			tokenizer.nextToken();
			if(tokenizer.hasMoreTokens())
			totalRawFeaturesInDocs = Integer.parseInt(tokenizer.nextToken());
			//System.out.println(k + ": " + feature + "\t" + totalDocsByFeat + "\n");
			idf = (float) Math.log10(totalDocs / (Float.MIN_VALUE + totalDocsByFeat)); 
			featIndex.put(k,idf);			
		}
		numOfFeatures = featIndex.size();
		//System.out.println(featIndex.size());
			
		

		featReader = readDataFile("/Users/dansaganome/Desktop/2-100/feature-sets/ne_all/docfeaturesets-weighted/docfeaturesets-weighted.xml");
		
		String partialXml = "";
		String fullXml = "";
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
