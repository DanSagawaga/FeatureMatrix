//package performance;

import java.util.Vector;

import org.la4j.matrix.Matrix;
import org.la4j.matrix.dense.Basic2DMatrix;
import utils.Formatting;

/**
 * Implement the concept of a confusion matrix for measuring performance. It is a square dense matrix.
 * Rows are expected class/concept labels.
 * Columns are predicted class/concept labels.
 * Most cases are square matrices of size 2:
 *                 predicted
 *                 C   notC
 * expected C      TP   FN
 *          notC   FP   TN
 * @author PATRICK
 *
 */
public class ConfusionMatrix {
	private Matrix matrix_;
	private Vector<ClassLabelData> classLabelData_ = new Vector<ClassLabelData>();;

	public ConfusionMatrix(int size) {
		initialize(size, null);
	}

	public ConfusionMatrix(int size, String[] labels) {
		initialize(size, labels);
	}

	private void initialize(int size, String[] labels) {
		// create the dense matrix
		matrix_ = new Basic2DMatrix(new double[size][size]);

		// initialize to 0
		for (int row=0; row<size; row++) {
			for (int col=0; col<size; col++) {
				matrix_.set(row, col, 0.0d);
			}
		}

		// create each class data
		for (int classIndex=0; classIndex<size; classIndex++) {
			if (labels==null) {
				classLabelData_.add(new ClassLabelData(classIndex, "C"+classIndex));
			}
			else {
				classLabelData_.add(new ClassLabelData(classIndex, labels[classIndex]));
			}
		}
	}

	public void set(int row, int col, double value) {
		matrix_.set(row, col, value);
		updateClassLabelData();
	}

	public void setRow(int row, Double[] values) {
		if (values.length!=classLabelData_.size()) return;
		for (int col=0; col<classLabelData_.size(); col++) {
			matrix_.set(row, col, values[col]);
		}
		updateClassLabelData();
	}

	public void setColumn(int col, Double[] values) {
		if (values.length!=classLabelData_.size()) return;
		for (int row=0; row<classLabelData_.size(); row++) {
			matrix_.set(row, col, values[col]);
		}
		updateClassLabelData();
	}

	public void display() {
		System.out.println("confusion matrix: (cols=predicted rows=expected)");

		// display column labels
		for (ClassLabelData cld : classLabelData_) System.out.print("\t"+cld.getLabel());
		System.out.println();

		// display the content of the matrix
		for (int row=0; row<matrix_.rows(); row++) {
			System.out.println(classLabelData_.get(row).getLabel() + "\t" + matrix_.getRow(row).toString());
		}

		// entire matrix stats and performance
		System.out.println("\nMatrix");
		System.out.println("\ttotal elements: " + getTotalElements());

		// display class stats and performance
		displayPerClassMeasures();
	}
	
	public String toString() {
		String output = "";
		output+=("confusion matrix: (cols=predicted rows=expected)\n");

		// display column labels
		for (ClassLabelData cld : classLabelData_) System.out.print("\t"+cld.getLabel());
		output+="\n";

		// display the content of the matrix
		for (int row=0; row<matrix_.rows(); row++) {
			output+=(classLabelData_.get(row).getLabel() + "\t" + matrix_.getRow(row).toString()+"\n");
		}

		// entire matrix stats and performance
		output+=("\nMatrix\n");
		output+=("\ttotal elements: " + getTotalElements()+"\n");

		// display class stats and performance
		output+=toStringClassMeasures();
		return output;
	}
	
	public String toStringClassMeasures() {
		String output = "";
		int upperBoundary = 0;
		if (classLabelData_.size()==2) {
			upperBoundary = 1;
		}
		else {
			output+=("Per class results measures:\n");
			upperBoundary = classLabelData_.size();
		}
		
		for (int classIndex=0; classIndex<upperBoundary; classIndex++) {
			output+=("\t" + classLabelData_.get(classIndex).getLabel());
		}
		output+="\n";
		output+=("count");
		for (int classIndex=0; classIndex<upperBoundary; classIndex++) {
			output+=("\t" + classLabelData_.get(classIndex).getTotalElements());
		}
		output+=("\ttotal count\n");
		output+=("TP");
		for (int classIndex=0; classIndex<upperBoundary; classIndex++) {
			output+=("\t" + Formatting.format(classLabelData_.get(classIndex).truePositives(), 3));
		}
		output+=("\tTrue Positives\n");
		output+=("FP");
		for (int classIndex=0; classIndex<upperBoundary; classIndex++) {
			output+=("\t" + Formatting.format(classLabelData_.get(classIndex).falsePositives(), 3));
		}
		output+=("\tFalse Positives\n");
		output+=("TN");
		for (int classIndex=0; classIndex<upperBoundary; classIndex++) {
			output+=("\t" + Formatting.format(classLabelData_.get(classIndex).trueNegatives(), 3));
		}
		output+=("\tTrue Negatives\n");
		output+=("FN");
		for (int classIndex=0; classIndex<upperBoundary; classIndex++) {
			output+=("\t" + Formatting.format(classLabelData_.get(classIndex).falseNegatives(), 3));
		}
		output+=("\tFalse Negatives\n");
		output+=("ACC");
		for (int classIndex=0; classIndex<upperBoundary; classIndex++) {
			output+=("\t" + Formatting.format(classLabelData_.get(classIndex).accuracy(), 3));
		}
		output+=("\tAccuracy\n");
		output+=("P");
		for (int classIndex=0; classIndex<upperBoundary; classIndex++) {
			output+=("\t" + Formatting.format(classLabelData_.get(classIndex).precision(), 3));
		}
		output+=("\tPrecision/Predictive Positive Value\n");
		output+=("R");
		for (int classIndex=0; classIndex<upperBoundary; classIndex++) {
			output+=("\t" + Formatting.format(classLabelData_.get(classIndex).recall(), 3));
		}
		output+=("\tRecall/Sensitivity/True Positive Rate\n");
		output+=("SPC");
		for (int classIndex=0; classIndex<upperBoundary; classIndex++) {
			output+=("\t" + Formatting.format(classLabelData_.get(classIndex).specificity(), 3));
		}
		output+=("\tSpecificity/True Negative Rate\n");
		output+=("F1");
		for (int classIndex=0; classIndex<upperBoundary; classIndex++) {
			output+=("\t" + Formatting.format(classLabelData_.get(classIndex).f1Measure(), 3));
		}
		output+=("\tF1-Measure\n");
		output+=("F2");
		for (int classIndex=0; classIndex<upperBoundary; classIndex++) {
			output+=("\t" + Formatting.format(classLabelData_.get(classIndex).f2Measure(), 3));
		}
		output+=("\tF2-Measure\n");
		output+=("F0.5");
		for (int classIndex=0; classIndex<upperBoundary; classIndex++) {
			output+=("\t" + Formatting.format(classLabelData_.get(classIndex).f05Measure(), 3));
		}
		output+=("\tF0.5-Measure\n");
		output+=("NPV");
		for (int classIndex=0; classIndex<upperBoundary; classIndex++) {
			output+=("\t" + Formatting.format(classLabelData_.get(classIndex).negativePredictionValue(), 3));
		}
		output+=("\tNegative Prediction Value\n");
		output+=("FPR");
		for (int classIndex=0; classIndex<upperBoundary; classIndex++) {
			output+=("\t" + Formatting.format(classLabelData_.get(classIndex).falsePredictiveRate(), 3));
		}
		output+=("\tFallout/False Positive Rate\n");
		output+=("FDR");
		for (int classIndex=0; classIndex<upperBoundary; classIndex++) {
			output+=("\t" + Formatting.format(classLabelData_.get(classIndex).falseDiscoveryRate(), 3));
		}
		output+=("\tFalse Discovery Rate\n");
		output+=("MCC");
		for (int classIndex=0; classIndex<upperBoundary; classIndex++) {
			output+=("\t" + Formatting.format(classLabelData_.get(classIndex).matthewsCorrelationCoefficient(), 3));
		}
		output+=("\tMatthews Correlation Coefficient\n");
		
		return output;
	}
	
	public void displayPerClassMeasures() {
		int upperBoundary = 0;
		if (classLabelData_.size()==2) {
			upperBoundary = 1;
		}
		else {
			System.out.println("Per class results measures:");
			upperBoundary = classLabelData_.size();
		}
		
		for (int classIndex=0; classIndex<upperBoundary; classIndex++) {
			System.out.print("\t" + classLabelData_.get(classIndex).getLabel());
		}
		System.out.println();
		System.out.print("count");
		for (int classIndex=0; classIndex<upperBoundary; classIndex++) {
			System.out.print("\t" + classLabelData_.get(classIndex).getTotalElements());
		}
		System.out.println("\ttotal count");
		System.out.print("TP");
		for (int classIndex=0; classIndex<upperBoundary; classIndex++) {
			System.out.print("\t" + Formatting.format(classLabelData_.get(classIndex).truePositives(), 3));
		}
		System.out.println("\tTrue Positives");
		System.out.print("FP");
		for (int classIndex=0; classIndex<upperBoundary; classIndex++) {
			System.out.print("\t" + Formatting.format(classLabelData_.get(classIndex).falsePositives(), 3));
		}
		System.out.println("\tFalse Positives");
		System.out.print("TN");
		for (int classIndex=0; classIndex<upperBoundary; classIndex++) {
			System.out.print("\t" + Formatting.format(classLabelData_.get(classIndex).trueNegatives(), 3));
		}
		System.out.println("\tTrue Negatives");
		System.out.print("FN");
		for (int classIndex=0; classIndex<upperBoundary; classIndex++) {
			System.out.print("\t" + Formatting.format(classLabelData_.get(classIndex).falseNegatives(), 3));
		}
		System.out.println("\tFalse Negatives");
		System.out.print("ACC");
		for (int classIndex=0; classIndex<upperBoundary; classIndex++) {
			System.out.print("\t" + Formatting.format(classLabelData_.get(classIndex).accuracy(), 3));
		}
		System.out.println("\tAccuracy");
		System.out.print("P");
		for (int classIndex=0; classIndex<upperBoundary; classIndex++) {
			System.out.print("\t" + Formatting.format(classLabelData_.get(classIndex).precision(), 3));
		}
		System.out.println("\tPrecision/Predictive Positive Value");
		System.out.print("R");
		for (int classIndex=0; classIndex<upperBoundary; classIndex++) {
			System.out.print("\t" + Formatting.format(classLabelData_.get(classIndex).recall(), 3));
		}
		System.out.println("\tRecall/Sensitivity/True Positive Rate");
		System.out.print("SPC");
		for (int classIndex=0; classIndex<upperBoundary; classIndex++) {
			System.out.print("\t" + Formatting.format(classLabelData_.get(classIndex).specificity(), 3));
		}
		System.out.println("\tSpecificity/True Negative Rate");
		System.out.print("F1");
		for (int classIndex=0; classIndex<upperBoundary; classIndex++) {
			System.out.print("\t" + Formatting.format(classLabelData_.get(classIndex).f1Measure(), 3));
		}
		System.out.println("\tF1-Measure");
		System.out.print("F2");
		for (int classIndex=0; classIndex<upperBoundary; classIndex++) {
			System.out.print("\t" + Formatting.format(classLabelData_.get(classIndex).f2Measure(), 3));
		}
		System.out.println("\tF2-Measure");
		System.out.print("F0.5");
		for (int classIndex=0; classIndex<upperBoundary; classIndex++) {
			System.out.print("\t" + Formatting.format(classLabelData_.get(classIndex).f05Measure(), 3));
		}
		System.out.println("\tF0.5-Measure");
		System.out.print("NPV");
		for (int classIndex=0; classIndex<upperBoundary; classIndex++) {
			System.out.print("\t" + Formatting.format(classLabelData_.get(classIndex).negativePredictionValue(), 3));
		}
		System.out.println("\tNegative Prediction Value");
		System.out.print("FPR");
		for (int classIndex=0; classIndex<upperBoundary; classIndex++) {
			System.out.print("\t" + Formatting.format(classLabelData_.get(classIndex).falsePredictiveRate(), 3));
		}
		System.out.println("\tFallout/False Positive Rate");
		System.out.print("FDR");
		for (int classIndex=0; classIndex<upperBoundary; classIndex++) {
			System.out.print("\t" + Formatting.format(classLabelData_.get(classIndex).falseDiscoveryRate(), 3));
		}
		System.out.println("\tFalse Discovery Rate");
		System.out.print("MCC");
		for (int classIndex=0; classIndex<upperBoundary; classIndex++) {
			System.out.print("\t" + Formatting.format(classLabelData_.get(classIndex).matthewsCorrelationCoefficient(), 3));
		}
		System.out.println("\tMatthews Correlation Coefficient");
	}

	private void updateClassLabelData() {
		ClassLabelData cld;
		for (int classIndex=0; classIndex<classLabelData_.size(); classIndex++) {
			cld = classLabelData_.get(classIndex);
			cld.setTotalElements((int) matrix_.getRow(classIndex).sum());
			int tp = (int) matrix_.get(classIndex, classIndex);
			int fp = (int) (matrix_.getColumn(classIndex).sum() - matrix_.get(classIndex, classIndex));
			int fn = (int) (matrix_.getRow(classIndex).sum() - matrix_.get(classIndex, classIndex));
			int tn = (int) (matrix_.sum() - tp - fp - fn);
			cld.setTruePositives(tp);
			cld.setFalsePositives(fp);
			cld.setTrueNegatives(tn);
			cld.setFalseNegatives(fn);
		}
	}

	public int getTotalElements() {
		int total = 0;
		for (int classIndex=0; classIndex<classLabelData_.size(); classIndex++) {
			total += classLabelData_.get(classIndex).getTotalElements();
		}
		return total;
	}

	class ClassLabelData {
		private int index_;
		private String label_;
		private int tp_=0, fp_=0, tn_=0, fn_=0;
		private int totalElements_ = 0;

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
		public String toString() {
			String output = "";
			output+=("\nClass " + label_);
			output+=("\tcount: " + totalElements_);
			output+=("\tTP: " + tp_);
			output+=("\tFP: " + fp_);
			output+=("\tTN: " + tn_);
			output+=("\tFN: " + fn_);
			output+=("\tACC: " + Formatting.format(accuracy(), 3));
			output+=("\tP: " + Formatting.format(precision(), 3));
			output+=("\tR: " + Formatting.format(recall(), 3));
			output+=("\tSPC: " + Formatting.format(specificity(), 3));
			output+=("\tF1: " + Formatting.format(f1Measure(), 3));
			output+=("\tF2: " + Formatting.format(f2Measure(), 3));
			output+=("\tF0.5: " + Formatting.format(f05Measure(), 3));
			output+=("\tNPV: " + Formatting.format(negativePredictionValue(), 3));
			output+=("\tFPR: " + Formatting.format(fallout(), 3));
			output+=("\tFDR: " + Formatting.format(falseDiscoveryRate(), 3));
			output+=("\tMCC: " + Formatting.format(matthewsCorrelationCoefficient(), 3));
			return output;
		}
	}

	/**
	 * demo/test
	 * 
	 * Expected values for TP, FP, TN, FN
	 * 		TP	FP	TN	FN
	 * A	30	4	46	20
	 * B	10	9	71	10
	 * C	25	22	48	5
	 * @param args
	 * @throws Exception
	 */
	public static void main(final String args[]) throws Exception {
		// create a confusion matrix with 3 class labels
		//ConfusionMatrix cm = new ConfusionMatrix(3);
		ConfusionMatrix cm = new ConfusionMatrix(3, new String[]{"A","B","C"});

		// fill the matrix with the test values
		cm.setRow(0, new Double[]{30d, 5d, 15d});
		cm.setRow(1, new Double[]{3d, 10d, 7d});
		cm.setRow(2, new Double[]{1d, 4d, 25d});
		cm.display();
	}
}
