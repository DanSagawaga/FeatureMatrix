import java.io.*;
import java.util.*;



public class Driver {

	static int numOfFeatures = 0;
	static int totalFeaturesInDocs = 0;
	static int totalDocswithFeat = 0;
	static int totalDocs= 0;
	static HashMap<String, Integer> featIndex = new HashMap<String, Integer>();


	public static void main (String args[]) throws IOException{
		
		BufferedReader featReader = readDataFile("/home/cloudera/Desktop/2-100/feature-sets/ne_all/dictionary/Part-00000.txt");
		String curLine = "";
		Scanner scanner; 
		
		
		// Reads dictionary to index features into a hash map
		for(int k = 0; ((curLine = featReader.readLine())!= null); k++){
			featIndex.put(curLine,k);			
		}
		numOfFeatures = featIndex.size();
		//System.out.println(featIndex.size());
		
		
		
		//Reads ClassMembership seq file to get total number of Documents
		featReader = readDataFile("/home/cloudera/Desktop/2-100/class-memberships/ClassMemberships.txt");
		while((curLine = featReader.readLine())!= null){
			totalDocs++;
		}
		System.out.println(totalDocs);
		
		

		featReader = readDataFile("/home/cloudera/Desktop/2-100/feature-sets/ne_all/docfeaturesets/DocFeatureSets.txt");
		
		int tempDocID = 0;
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
				XmlReader = XmlReader.createFromXmlString(fullXml);
				System.out.println(fullXml);
			}
		
			//System.out.println(partialXml);
	
		//input = input + "\n" +curLine;
		}
		//System.out.println(fullXml);
		

	//	FeatureSetWeighted setReader = new FeatureSetWeighted();
	//	setReader = setReader.createFromXmlString(curLine);
		//System.out.println(SetReader.toString());
		
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
