package TOPK;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by kingkz on 11/13/16.
 */

public class DescendingKeyComparator extends WritableComparator {
    protected DescendingKeyComparator() {
        super(DoubleWritable.class, true);
    }


    /*
        Simple, just reverse the original comparing result
     */
    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        DoubleWritable key1 = (DoubleWritable) w1;
        DoubleWritable key2 = (DoubleWritable) w2;
        return -1 * key1.compareTo(key2);
    }
}