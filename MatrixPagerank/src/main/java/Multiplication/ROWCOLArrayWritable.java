package Multiplication;

import com.google.common.collect.Iterables;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by kingkz on 11/12/16.
 */
public class ROWCOLArrayWritable extends ArrayWritable {
    public ROWCOLArrayWritable(Iterable<ROWCOLWritable> rowcols) {
        super(ROWCOLArrayWritable.class);
        set(Iterables.toArray(rowcols, ROWCOLWritable.class));
    }
}
