package Preprocess;

import com.google.common.collect.Iterables;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by kingkz on 11/11/16.
 */
public class CellArrayWritable extends ArrayWritable {

    public CellArrayWritable(CellWritable[] cells) {
        super(CellWritable.class);
        set(cells);
    }

    public CellArrayWritable() {
        super(CellWritable.class);
        set(new CellWritable[1]);
    }

    public CellArrayWritable(Iterable<CellWritable> cells) {
        super(CellWritable.class);
        set(Iterables.toArray(cells, CellWritable.class));
    }

    public CellArrayWritable(CellArrayWritable cellArrayWritable) {
        super(CellWritable.class);
        CellWritable[] cells = new CellWritable[cellArrayWritable.get().length];
        int idx = 0;
        for (Writable w : cellArrayWritable.get()) {
            CellWritable temp = (CellWritable) w;
            CellWritable cellWritable = new CellWritable(temp.getRowcol(),
                    temp.getValue());
            cells[idx++] = cellWritable;
        }
        set(cells);
    }

}
