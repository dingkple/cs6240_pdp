package Multiplication;

import Preprocess.CellArrayWritable;
import Preprocess.CellWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by kingkz on 11/12/16.
 */
public class ROWCOLWritable implements Writable {

    private long id;
    private CellArrayWritable cellArray;

    public ROWCOLWritable(long id, CellArrayWritable cellArray) {

        this. id = id;
        CellWritable[] array = new CellWritable[cellArray.get().length];
        int idx = 0;
        for (Writable w : cellArray.get()) {
            CellWritable cell = new CellWritable((CellWritable) w);
            array[idx++] = cell;
        }
        this.cellArray = new CellArrayWritable(array);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(id);
        cellArray.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readLong();
        cellArray.readFields(in);
    }
}
