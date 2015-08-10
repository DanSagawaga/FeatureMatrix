
import org.apache.commons.lang3.StringEscapeUtils;
import org.jdom2.Element;

import utils.Formatting;

public class ClassLabelData {
	private int index_;
	private String label_;
	private int tp_=0, fp_=0, tn_=0, fn_=0;
	private int totalElements_ = 0;
	public static final String XML_CLASS_LABEL_DATA_HEADER = "cld";
	public static final String XML_CLASS_LABEL_DATA_INDEX = "ndx";
	public static final String XML_CLASS_LABEL_DATA_LABEL = "lbl";
	public static final String XML_CLASS_LABEL_DATA_TP = "tp";
	public static final String XML_CLASS_LABEL_DATA_FP = "fp";
	public static final String XML_CLASS_LABEL_DATA_TN = "tn";
	public static final String XML_CLASS_LABEL_DATA_FN = "fn";
	public static final String XML_CLASS_LABEL_DATA_TOTAL_ELEMENTS = "n";

	public ClassLabelData(int index, String label) {
		index_ = index;
		label_ = label;
	}

	public int getIndex() {
		return index_;
	}

	public String getLabel() {
		return label_;
	}

	public void setTotalElements(int totalElements) {
		totalElements_ = totalElements;
	}

	public int getTotalElements() {
		return totalElements_;
	}

	public void setTruePositives(int tp) {
		tp_ = tp;
	}

	public int truePositives() {
		return tp_;
	}

	public void setFalsePositives(int fp) {
		fp_ = fp;
	}

	public int falsePositives() {
		return fp_;
	}

	public void setTrueNegatives(int tn) {
		tn_ = tn;
	}

	public int trueNegatives() {
		return tn_;
	}

	public void setFalseNegatives(int fn) {
		fn_ = fn;
	}

	public int falseNegatives() {
		return fn_;
	}

	public double accuracy() {
		double den = tp_ + fp_ + tn_ + fn_;
		if (den==0) return 0;
		double num = tp_ + tn_;
		return num / den;
	}

	public double precision() {
		double den = tp_ + fp_;
		if (den==0.0) return 0d;
		return (double) tp_ / den;
	}

	/**
	 * Same as precision
	 * @return
	 */
	public double predictivePositiveValue() {
		return precision();
	}

	public double recall() {
		double den = tp_ + fn_;
		if (den==0.0) return 0d;
		return (double) tp_ / den;
	}

	/**
	 * Same as recall
	 * @return
	 */
	public double sensitivity() {
		return recall();
	}

	/**
	 * Same as recall
	 * @return
	 */
	public double truePositiveRate() {
		return recall();
	}

	public double specificity() {
		double den = fp_ + tn_;
		if (den==0.0) return 0d;
		return (double) tn_ / den;
	}

	/**
	 * Same as specificity
	 * @return
	 */
	public double trueNegativeRate() {
		return specificity();
	}

	public double f1Measure() {
		return 2*prratio();
	}

	public double f2Measure() {
		return 1.25*prratio();
	}

	public double f05Measure() {
		return 5.04*prratio();
	}

	private double prratio() {
		double p = precision();
		double r = recall();
		double den = p+r;
		if (den==0) return 0;
		double num = p*r; 
		return num/den;
	}

	public double negativePredictionValue() {
		double den = tn_ + fn_;
		if (den==0) return 0;
		return (double) tn_ / den;
	}

	public double fallout() {
		double den = fp_ + tn_;
		if (den==0) return 0;
		return (double) fp_ / den;
	}

	/**
	 * Same as fallout
	 * @return
	 */
	public double falsePredictiveRate() {
		return fallout();
	}

	public double falseDiscoveryRate() {
		double den = fp_ + tp_;
		if (den==0) return 0;
		return (double) fp_ / den;
	}

	public double matthewsCorrelationCoefficient() {
		double den = Math.sqrt((tp_ + fp_) * (tp_ + fn_) * (tn_ + fp_) * (tn_ + fn_));
		if (den==0) return 0;
		double num = (tp_ * tn_) - (fp_ * fn_);
		return (double) num / den;
	}

	public void display() {
		System.out.println("\nClass " + label_);
		System.out.println("\tcount: " + totalElements_);
		System.out.println("\tTP: " + tp_);
		System.out.println("\tFP: " + fp_);
		System.out.println("\tTN: " + tn_);
		System.out.println("\tFN: " + fn_);
		System.out.println("\tACC: " + Formatting.format(accuracy(), 3));
		System.out.println("\tP: " + Formatting.format(precision(), 3));
		System.out.println("\tR: " + Formatting.format(recall(), 3));
		System.out.println("\tSPC: " + Formatting.format(specificity(), 3));
		System.out.println("\tF1: " + Formatting.format(f1Measure(), 3));
		System.out.println("\tF2: " + Formatting.format(f2Measure(), 3));
		System.out.println("\tF0.5: " + Formatting.format(f05Measure(), 3));
		System.out.println("\tNPV: " + Formatting.format(negativePredictionValue(), 3));
		System.out.println("\tFPR: " + Formatting.format(fallout(), 3));
		System.out.println("\tFDR: " + Formatting.format(falseDiscoveryRate(), 3));
		System.out.println("\tMCC: " + Formatting.format(matthewsCorrelationCoefficient(), 3));
	}

	@SuppressWarnings("deprecation")
	public String xml() {
		StringBuilder xmlstr = new StringBuilder("");
		xmlstr.append("<").append(XML_CLASS_LABEL_DATA_HEADER).append("");
		xmlstr.append(" ").append(XML_CLASS_LABEL_DATA_INDEX).append("=\"").append(index_).append("\"");
		xmlstr.append(" ").append(XML_CLASS_LABEL_DATA_LABEL).append("=\"").append(StringEscapeUtils.escapeXml(label_)).append("\"");
		xmlstr.append(" ").append(XML_CLASS_LABEL_DATA_TP).append("=\"").append(tp_).append("\"");
		xmlstr.append(" ").append(XML_CLASS_LABEL_DATA_FP).append("=\"").append(fp_).append("\"");
		xmlstr.append(" ").append(XML_CLASS_LABEL_DATA_TN).append("=\"").append(tn_).append("\"");
		xmlstr.append(" ").append(XML_CLASS_LABEL_DATA_FN).append("=\"").append(fn_).append("\"");
		xmlstr.append(" ").append(XML_CLASS_LABEL_DATA_TOTAL_ELEMENTS).append("=\"").append(totalElements_).append("\"");
		xmlstr.append("/>\n");
		return xmlstr.toString();
	}

	public static ClassLabelData createFromXmlString(Element e) {
		if (e.getName().equalsIgnoreCase(XML_CLASS_LABEL_DATA_HEADER)) {
			try {
				ClassLabelData cld = new ClassLabelData(
						Integer.parseInt(e.getAttributeValue(XML_CLASS_LABEL_DATA_INDEX)), 
						e.getAttributeValue(XML_CLASS_LABEL_DATA_LABEL));
				cld.setTruePositives(Integer.parseInt(e.getAttributeValue(XML_CLASS_LABEL_DATA_TP)));
				cld.setFalsePositives(Integer.parseInt(e.getAttributeValue(XML_CLASS_LABEL_DATA_FP)));
				cld.setTrueNegatives(Integer.parseInt(e.getAttributeValue(XML_CLASS_LABEL_DATA_TN)));
				cld.setFalseNegatives(Integer.parseInt(e.getAttributeValue(XML_CLASS_LABEL_DATA_FN)));
				cld.setTotalElements(Integer.parseInt(e.getAttributeValue(XML_CLASS_LABEL_DATA_TOTAL_ELEMENTS)));
				return cld;
			}
			catch (NumberFormatException nfe) {
				System.out.println(nfe.getMessage());
			}
		}
		return null;
	}
}
