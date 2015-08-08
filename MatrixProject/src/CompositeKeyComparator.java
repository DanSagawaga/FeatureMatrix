import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

public class CompositeKeyComparator extends WritableComparator {
    protected CompositeKeyComparator() {
        super(CompositeKey.class, true);
    }   
    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        CompositeKey k1 = (CompositeKey)w1;
        CompositeKey k2 = (CompositeKey)w2;
         
        int result = k1.getPrimaryKey().compareTo(k2.getPrimaryKey());
        if(0 == result) {
            result =  k1.getSecondaryKey().compareTo(k2.getSecondaryKey());
        }
        return result;
    }
}