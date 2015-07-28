import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import weka.core.Instance;
import weka.core.InstanceComparator;
import weka.core.Instances;
import weka.core.converters.ArffLoader.ArffReader;
import weka.core.converters.ConverterUtils.DataSource;
import weka.core.Utils;
import weka.classifiers.AbstractClassifier;
import weka.classifiers.AggregateableEvaluation;
import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.trees.j48.*;
import weka.classifiers.rules.*;
import weka.classifiers.trees.*;
import weka.classifiers.bayes.*;
import weka.classifiers.lazy.*;
import weka.classifiers.evaluation.ConfusionMatrix;
import weka.core.SparseInstance;
import weka.core.Attribute;

public class ClassifierKey implements WritableComparable<ClassifierKey> {

	private String primaryKey;
	private Classifier[] models = {new J48(),new PART(), new DecisionTable(), new LMT(),new DecisionStump(), //one-level decision tree new NaiveBayes(), new AODE(), new BayesNet(),new KStar(), new IBk(), new LWL()		
	};
	
	
	public void trainClassifiers(Instances parDataset){
		
	}
	
	public ClassifierKey(String primaryKeyPar) {
		this.primaryKey = primaryKeyPar;
	}
	
	
	
	
	@Override
	public void readFields(DataInput in) throws IOException {
		primaryKey = WritableUtils.readString(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, primaryKey);
	}

	@Override
	public int compareTo(ClassifierKey o) {
		int result = primaryKey.compareTo(o.primaryKey);
		if(0 == result) {
			return 1;
		}
		return result;
	}
}
