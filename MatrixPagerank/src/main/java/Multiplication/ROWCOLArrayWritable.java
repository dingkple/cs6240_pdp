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
        super(ROWCOLWritable.class);
        set(Iterables.toArray(rowcols, ROWCOLWritable.class));
    }

    public ROWCOLArrayWritable(ROWCOLWritable[] data) {
        super(ROWCOLWritable.class);
        set(data);
    }

    public ROWCOLArrayWritable() {
        super(ROWCOLWritable.class);
        set(new ROWCOLWritable[0]);
    }
}
