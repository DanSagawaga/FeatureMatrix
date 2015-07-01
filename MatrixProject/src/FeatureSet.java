import java.io.IOException;
import java.io.StringReader;
import java.util.HashSet;

import org.apache.commons.lang3.StringEscapeUtils;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

public class FeatureSet {
	public static final String XML_TAG_FEATURE_SET_HEADER = "features";
	public static final String XML_TAG_FEATURE = "feature";
	public static final String XML_TAG_FEATURE_NAME = "name";
	private HashSet<String> features_ = new HashSet<String>();

	public void add(String feature) {
		if (feature==null) return;
		if (feature.isEmpty()) return;
		features_.add(feature);
	}

	public boolean contains(String feature) {
		return features_.contains(feature);
	}
	
	public String toString(){
		return features_.toString();
	}

	@SuppressWarnings("deprecation")
	public String xml() {
		StringBuilder xmlstr = new StringBuilder("");
		xmlstr.append("<").append(XML_TAG_FEATURE_SET_HEADER).append(">\n");
		for (String feature : features_) {
			xmlstr.append("<").append(XML_TAG_FEATURE).append("");
			xmlstr.append(" ").append(XML_TAG_FEATURE_NAME).append("=\"").append(StringEscapeUtils.escapeXml(feature)).append("\"");
			xmlstr.append("/>\n");

		}
		xmlstr.append("</").append(XML_TAG_FEATURE_SET_HEADER).append(">\n");
		return xmlstr.toString();
	}

	public static FeatureSet createFromXmlString(String xmlString) {
		FeatureSet fs = new FeatureSet();
		try {
			Document doc = new SAXBuilder().build(new StringReader(xmlString));
			if (doc!=null) {
				for (Element child : doc.getRootElement().getChildren()) {
					if (child.getName().equalsIgnoreCase(XML_TAG_FEATURE)) {
						fs.add(child.getAttribute(XML_TAG_FEATURE_NAME).getValue());
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

	
}

