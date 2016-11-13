package Preprocess;

import com.google.common.collect.Iterables;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;

/**
 * Created by kingkz on 11/11/16.
 */
public class TupleArrayWritable extends ArrayWritable {


    public TupleArrayWritable(Iterable<TupleWritable> tuples) {
        super(TupleWritable.class);
        set(Iterables.toArray(tuples, TupleWritable.class));
    }

    public TupleArrayWritable(TupleWritable[] arr) {
        super(TupleWritable.class);
        set(arr);
    }
}
