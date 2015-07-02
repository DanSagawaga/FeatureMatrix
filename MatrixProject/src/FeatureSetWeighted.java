import java.io.IOException;
import java.io.StringReader;
import java.util.TreeMap;

import org.apache.commons.lang3.StringEscapeUtils;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

public class FeatureSetWeighted {
	public static final String XML_TAG_FEATURE_WEIGHT = "weight";
	private TreeMap<String,Double> weightedFeatures_ = new TreeMap<String,Double>();
	
	public void add(String feature, double weight) {
		if (feature==null) return;
		if (feature.isEmpty()) return;
		if (weightedFeatures_.containsKey(feature)) weightedFeatures_.put(feature, weightedFeatures_.get(feature)+weight);
		else weightedFeatures_.put(feature, weight);
	}
	
	public void replace(String feature, double weight) {
		if (feature==null) return;
		if (feature.isEmpty()) return;
		weightedFeatures_.put(feature, weight);
	}
	
	public boolean contains(String feature) {
		return weightedFeatures_.containsKey(feature);
	}
	
	public double getWeight(String feature) {
		return weightedFeatures_.get(feature);
	}
	
	@SuppressWarnings("deprecation")
	public String xml() {
		StringBuilder xmlstr = new StringBuilder("");
		xmlstr.append("<").append(FeatureSet.XML_TAG_FEATURE_SET_HEADER).append(">\n");
		for (String feature : weightedFeatures_.keySet()) {
			xmlstr.append("<").append(FeatureSet.XML_TAG_FEATURE).append("");
			xmlstr.append(" ").append(FeatureSet.XML_TAG_FEATURE_NAME).append("=\"").append(StringEscapeUtils.escapeXml(feature)).append("\"");
			xmlstr.append(" ").append(XML_TAG_FEATURE_WEIGHT).append("=\"").append(getWeight(feature)).append("\"");
			xmlstr.append("/>\n");

		}
		xmlstr.append("</").append(FeatureSet.XML_TAG_FEATURE_SET_HEADER).append(">\n");
		return xmlstr.toString();
	}
	public FeatureSetWeighted(){
		
	}
	public FeatureSetWeighted (String xmlString) {
		try {
			Document doc = new SAXBuilder().build(new StringReader(xmlString));
			if (doc!=null) {
				for (Element child : doc.getRootElement().getChildren()) {
					if (child.getName().equalsIgnoreCase(FeatureSet.XML_TAG_FEATURE)) {
						add(child.getAttribute(FeatureSet.XML_TAG_FEATURE_NAME).getValue(),
								Double.parseDouble(child.getAttribute(XML_TAG_FEATURE_WEIGHT).getValue()));
					}
				}
			}
		} 
		catch (JDOMException jdome) {
			System.err.println("JDOMException " + jdome.getMessage());
		} 
		catch (IOException ioe) {
			System.err.println("IOException " + ioe.getMessage());
		}
	}
}
/*



	public static FeatureSetWeighted createFromXmlString(String xmlString) {
		FeatureSetWeighted fs = new FeatureSetWeighted();
		try {
			Document doc = new SAXBuilder().build(new StringReader(xmlString));
			if (doc!=null) {
				for (Element child : doc.getRootElement().getChildren()) {
					if (child.getName().equalsIgnoreCase(FeatureSet.XML_TAG_FEATURE)) {
						fs.add(child.getAttribute(FeatureSet.XML_TAG_FEATURE_NAME).getValue(),
								Double.parseDouble(child.getAttribute(XML_TAG_FEATURE_WEIGHT).getValue()));
					}
				}
			}
		} 
		catch (JDOMException jdome) {
			System.err.println("JDOMException " + jdome.getMessage());
		} 
		catch (IOException ioe) {
			System.err.println("IOException " + ioe.getMessage());
		}
		return fs;
	}







*/