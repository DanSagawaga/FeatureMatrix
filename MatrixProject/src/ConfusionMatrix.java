import java.io.IOException;
import java.io.PrintStream;
import java.io.Reader;
import java.io.StringReader;
import java.util.List;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.la4j.matrix.Matrix;
import org.la4j.matrix.dense.Basic2DMatrix;
import org.la4j.vector.Vector;
import performance.ClassLabelData;
import utils.Formatting;

public class ConfusionMatrix {
	private Matrix matrix_;
	private java.util.Vector<ClassLabelData> classLabelData_ = new java.util.Vector();
	public static final String XML_CONFUSION_MATRIX_HEADER = "cf";
	public static final String XML_CONFUSION_MATRIX_SIZE = "msize";
	public static final String XML_CONFUSION_MATRIX_TOTAL_ROWS = "totr";
	public static final String XML_CONFUSION_MATRIX_TOTAL_COLUMNS = "totc";
	public static final String XML_CLASS_LABEL_DATA = "cldata";
	public static final String XML_CONFUSION_MATRIX_DATA = "mdata";
	public static final String XML_CONFUSION_MATRIX_CELL = "mcell";
	public static final String XML_CONFUSION_MATRIX_ROW = "r";
	public static final String XML_CONFUSION_MATRIX_COLUMN = "c";
	public static final String XML_CONFUSION_MATRIX_VALUE = "v";

	public ConfusionMatrix(int size) {
		this.initialize(size, null);
	}

	public ConfusionMatrix(int size, String[] labels) {
		this.initialize(size, labels);
	}

	private void initialize(int size, String[] labels) {
		this.matrix_ = new Basic2DMatrix(new double[size][size]);
		for (int row = 0; row < size; ++row) {
			for (int col = 0; col < size; ++col) {
				this.matrix_.set(row, col, 0.0);
			}
		}
		for (int classIndex = 0; classIndex < size; ++classIndex) {
			if (labels == null) {
				this.classLabelData_.add(new ClassLabelData(classIndex, "C" + classIndex));
				continue;
			}
			this.classLabelData_.add(new ClassLabelData(classIndex, labels[classIndex]));
		}
	}

	public ConfusionMatrix(Matrix matrix, java.util.Vector<ClassLabelData> classLabelData) {
		this.matrix_ = matrix;
		this.classLabelData_ = classLabelData;
	}

	public void set(int row, int col, double value) {
		this.matrix_.set(row, col, value);
		this.updateClassLabelData();
	}

	public void setRow(int row, Double[] values) {
		if (values.length != this.classLabelData_.size()) {
			return;
		}
		for (int col = 0; col < this.classLabelData_.size(); ++col) {
			this.matrix_.set(row, col, values[col].doubleValue());
		}
		this.updateClassLabelData();
	}

	public void setColumn(int col, Double[] values) {
		if (values.length != this.classLabelData_.size()) {
			return;
		}
		for (int row = 0; row < this.classLabelData_.size(); ++row) {
			this.matrix_.set(row, col, values[col].doubleValue());
		}
		this.updateClassLabelData();
	}

	public void display() {
		System.out.println("confusion matrix: (cols=predicted rows=expected)");
		for (ClassLabelData cld : this.classLabelData_) {
			System.out.print("\t" + cld.getLabel());
		}
		System.out.println();
		for (int row = 0; row < this.matrix_.rows(); ++row) {
			System.out.println(String.valueOf(this.classLabelData_.get(row).getLabel()) + "\t" + this.matrix_.getRow(row).toString());
		}
		System.out.println("\nMatrix");
		System.out.println("\ttotal elements: " + this.getTotalElements());
		this.displayPerClassMeasures();
	}

	public void displayPerClassMeasures() {
		int classIndex;
		int upperBoundary = 0;
		if (this.classLabelData_.size() == 2) {
			upperBoundary = 2;
		} else {
			System.out.println("Per class results measures:");
			upperBoundary = this.classLabelData_.size();
		}
		for (classIndex = 0; classIndex < upperBoundary; ++classIndex) {
			System.out.print("\t" + this.classLabelData_.get(classIndex).getLabel());
		}
		System.out.println();
		System.out.print("count");
		for (classIndex = 0; classIndex < upperBoundary; ++classIndex) {
			System.out.print("\t" + this.classLabelData_.get(classIndex).getTotalElements());
		}
		System.out.println("\ttotal count");
		System.out.print("TP");
		for (classIndex = 0; classIndex < upperBoundary; ++classIndex) {
			System.out.print("\t" + Formatting.format((double)this.classLabelData_.get(classIndex).truePositives(), (int)3));
		}
		System.out.println("\tTrue Positives");
		System.out.print("FP");
		for (classIndex = 0; classIndex < upperBoundary; ++classIndex) {
			System.out.print("\t" + Formatting.format((double)this.classLabelData_.get(classIndex).falsePositives(), (int)3));
		}
		System.out.println("\tFalse Positives");
		System.out.print("TN");
		for (classIndex = 0; classIndex < upperBoundary; ++classIndex) {
			System.out.print("\t" + Formatting.format((double)this.classLabelData_.get(classIndex).trueNegatives(), (int)3));
		}
		System.out.println("\tTrue Negatives");
		System.out.print("FN");
		for (classIndex = 0; classIndex < upperBoundary; ++classIndex) {
			System.out.print("\t" + Formatting.format((double)this.classLabelData_.get(classIndex).falseNegatives(), (int)3));
		}
		System.out.println("\tFalse Negatives");
		System.out.print("ACC");
		for (classIndex = 0; classIndex < upperBoundary; ++classIndex) {
			System.out.print("\t" + Formatting.format((double)this.classLabelData_.get(classIndex).accuracy(), (int)3));
		}
		System.out.println("\tAccuracy");
		System.out.print("P");
		for (classIndex = 0; classIndex < upperBoundary; ++classIndex) {
			System.out.print("\t" + Formatting.format((double)this.classLabelData_.get(classIndex).precision(), (int)3));
		}
		System.out.println("\tPrecision/Predictive Positive Value");
		System.out.print("R");
		for (classIndex = 0; classIndex < upperBoundary; ++classIndex) {
			System.out.print("\t" + Formatting.format((double)this.classLabelData_.get(classIndex).recall(), (int)3));
		}
		System.out.println("\tRecall/Sensitivity/True Positive Rate");
		System.out.print("SPC");
		for (classIndex = 0; classIndex < upperBoundary; ++classIndex) {
			System.out.print("\t" + Formatting.format((double)this.classLabelData_.get(classIndex).specificity(), (int)3));
		}
		System.out.println("\tSpecificity/True Negative Rate");
		System.out.print("F1");
		for (classIndex = 0; classIndex < upperBoundary; ++classIndex) {
			System.out.print("\t" + Formatting.format((double)this.classLabelData_.get(classIndex).f1Measure(), (int)3));
		}
		System.out.println("\tF1-Measure");
		System.out.print("F2");
		for (classIndex = 0; classIndex < upperBoundary; ++classIndex) {
			System.out.print("\t" + Formatting.format((double)this.classLabelData_.get(classIndex).f2Measure(), (int)3));
		}
		System.out.println("\tF2-Measure");
		System.out.print("F0.5");
		for (classIndex = 0; classIndex < upperBoundary; ++classIndex) {
			System.out.print("\t" + Formatting.format((double)this.classLabelData_.get(classIndex).f05Measure(), (int)3));
		}
		System.out.println("\tF0.5-Measure");
		System.out.print("NPV");
		for (classIndex = 0; classIndex < upperBoundary; ++classIndex) {
			System.out.print("\t" + Formatting.format((double)this.classLabelData_.get(classIndex).negativePredictionValue(), (int)3));
		}
		System.out.println("\tNegative Prediction Value");
		System.out.print("FPR");
		for (classIndex = 0; classIndex < upperBoundary; ++classIndex) {
			System.out.print("\t" + Formatting.format((double)this.classLabelData_.get(classIndex).falsePredictiveRate(), (int)3));
		}
		System.out.println("\tFallout/False Positive Rate");
		System.out.print("FDR");
		for (classIndex = 0; classIndex < upperBoundary; ++classIndex) {
			System.out.print("\t" + Formatting.format((double)this.classLabelData_.get(classIndex).falseDiscoveryRate(), (int)3));
		}
		System.out.println("\tFalse Discovery Rate");
		System.out.print("MCC");
		for (classIndex = 0; classIndex < upperBoundary; ++classIndex) {
			System.out.print("\t" + Formatting.format((double)this.classLabelData_.get(classIndex).matthewsCorrelationCoefficient(), (int)3));
		}
		System.out.println("\tMatthews Correlation Coefficient");
	}

	private void updateClassLabelData() {
		for (int classIndex = 0; classIndex < this.classLabelData_.size(); ++classIndex) {
			ClassLabelData cld = this.classLabelData_.get(classIndex);
			cld.setTotalElements((int)this.matrix_.getRow(classIndex).sum());
			int tp = (int)this.matrix_.get(classIndex, classIndex);
			int fp = (int)(this.matrix_.getColumn(classIndex).sum() - this.matrix_.get(classIndex, classIndex));
			int fn = (int)(this.matrix_.getRow(classIndex).sum() - this.matrix_.get(classIndex, classIndex));
			int tn = (int)(this.matrix_.sum() - (double)tp - (double)fp - (double)fn);
			cld.setTruePositives(tp);
			cld.setFalsePositives(fp);
			cld.setTrueNegatives(tn);
			cld.setFalseNegatives(fn);
		}
	}

	public String getFMeasures(){
		int classIndex = 0;
		int upperBoundary = 0;
		String output = "";
		upperBoundary = this.classLabelData_.size();
			
		for (classIndex = 0; classIndex < upperBoundary; ++classIndex) {
			output += this.classLabelData_.get(classIndex).getLabel()
					+"\t"+Formatting.format((double)this.classLabelData_.get(classIndex).f05Measure() ,(int)3)
					+"\t"+ Formatting.format((double)this.classLabelData_.get(classIndex).f1Measure() ,(int)3)
					+"\t"+ Formatting.format((double)this.classLabelData_.get(classIndex).f2Measure() ,(int)3)+"\n";

		}
		return output;
	}
	
	public double getAverageF1Score(){
		int classIndex = 0;
		double avg = 0;
		for (classIndex = 0; classIndex < this.classLabelData_.size(); ++classIndex) {
			avg += this.classLabelData_.get(classIndex).f1Measure();
		}
		return avg/this.classLabelData_.size();
	}
	public double getAverageF2Score(){
		int classIndex = 0;
		double avg = 0;
		for (classIndex = 0; classIndex < this.classLabelData_.size(); ++classIndex) {
			avg += this.classLabelData_.get(classIndex).f2Measure();
		}
		return avg/this.classLabelData_.size();
	}
	public double getAverageF05Score(){
		int classIndex = 0;
		double avg = 0;
		for (classIndex = 0; classIndex < this.classLabelData_.size(); ++classIndex) {
			avg += this.classLabelData_.get(classIndex).f05Measure();
		}
		return avg/this.classLabelData_.size();
	}

	public int getTotalElements() {
		int total = 0;
		for (int classIndex = 0; classIndex < this.classLabelData_.size(); ++classIndex) {
			total+=this.classLabelData_.get(classIndex).getTotalElements();
		}
		return total;
	}

	public String xml() {
		StringBuilder xmlstr = new StringBuilder("");
		xmlstr.append("<").append("cf").append(">\n");
		xmlstr.append("<").append("msize").append("");
		xmlstr.append(" ").append("totr").append("=\"").append(this.matrix_.rows()).append("\"");
		xmlstr.append(" ").append("totc").append("=\"").append(this.matrix_.columns()).append("\"");
		xmlstr.append("/>\n");
		xmlstr.append("<").append("mdata").append(">\n");
		for (int row = 0; row < this.matrix_.rows(); ++row) {
			for (int col = 0; col < this.matrix_.columns(); ++col) {
				xmlstr.append("<").append("mcell").append("");
				xmlstr.append(" ").append("r").append("=\"").append(row).append("\"");
				xmlstr.append(" ").append("c").append("=\"").append(col).append("\"");
				xmlstr.append(" ").append("v").append("=\"").append(this.matrix_.get(row, col)).append("\"");
				xmlstr.append("/>\n");
			}
		}
		xmlstr.append("</").append("mdata").append(">\n");
		xmlstr.append("<").append("cldata").append(">\n");
		for (ClassLabelData cld : this.classLabelData_) {
			xmlstr.append(cld.xml());
		}
		xmlstr.append("</").append("cldata").append(">\n");
		return xmlstr.append("</").append("cf").append(">\n").toString();
	}

	public static ConfusionMatrix createFromXmlString(String xmlString) {
		try {
			Document doc = new SAXBuilder().build((Reader)new StringReader(xmlString));
			if (doc != null) {
				Element matrixSizeDefinition = null;
				Element matrixContentDefinition = null;
				Element classLabelDataDefinition = null;
				for (Element child : doc.getRootElement().getChildren()) {
					if (child.getName().equalsIgnoreCase("msize")) {
						matrixSizeDefinition = child;
						continue;
					}
					if (child.getName().equalsIgnoreCase("mdata")) {
						matrixContentDefinition = child;
						continue;
					}
					if (!child.getName().equalsIgnoreCase("cldata")) continue;
					classLabelDataDefinition = child;
				}
				if (matrixSizeDefinition != null && matrixContentDefinition != null && classLabelDataDefinition != null) {
					int totalRows = Integer.parseInt(matrixSizeDefinition.getAttributeValue("totr"));
					int totalColumns = Integer.parseInt(matrixSizeDefinition.getAttributeValue("totc"));
					Basic2DMatrix matrix = new Basic2DMatrix(new double[totalRows][totalColumns]);
					for (Element child2 : matrixContentDefinition.getChildren()) {
						if (!child2.getName().equalsIgnoreCase("mcell")) continue;
						matrix.set(Integer.parseInt(child2.getAttributeValue("r")), Integer.parseInt(child2.getAttributeValue("c")), Double.parseDouble(child2.getAttributeValue("v")));
					}
					java.util.Vector<ClassLabelData> classLabelData = new java.util.Vector<ClassLabelData>();
					for (Element child3 : classLabelDataDefinition.getChildren()) {
						classLabelData.add(ClassLabelData.createFromXmlString((Element)child3));
					}
					return new ConfusionMatrix((Matrix)matrix, classLabelData);
				}
			}
		}
		catch (IOException | NumberFormatException | JDOMException e) {
			System.err.println(e.getMessage());
		}
		return null;
	}

	public static void main(String[] args) throws Exception {
		System.out.println("------------------------------------- create a matrix -----------------");
		ConfusionMatrix cm = new ConfusionMatrix(3, new String[]{"A", "B", "C"});
		cm.setRow(0, new Double[]{30.0, 5.0, 15.0});
		cm.setRow(1, new Double[]{3.0, 10.0, 7.0});
		cm.setRow(2, new Double[]{1.0, 4.0, 25.0});
		cm.display();
		System.out.println("\n\n------------------------------------- test write/read matrix -----------------");
		System.out.println(cm.xml());
		ConfusionMatrix cm_copy = ConfusionMatrix.createFromXmlString(cm.xml());
		cm_copy.display();
	}
}
