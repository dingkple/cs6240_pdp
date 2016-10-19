package TopLinks;

import org.apache.hadoop.io.*;

/**
 * Created by kingkz on 10/17/16.
 */
public class DescendingKeyComparator extends WritableComparator {
    protected DescendingKeyComparator() {
        super(DoubleWritable.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        DoubleWritable key1 = (DoubleWritable) w1;
        DoubleWritable key2 = (DoubleWritable) w2;
        return -1 * key1.compareTo(key2);
    }
}