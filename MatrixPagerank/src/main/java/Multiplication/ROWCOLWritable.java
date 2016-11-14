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

    private int id;
    private CellArrayWritable cellArray;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public CellArrayWritable getCellArray() {
        return cellArray;
    }

    public void setCellArray(CellArrayWritable cellArray) {
        this.cellArray = cellArray;
    }

    public ROWCOLWritable() {
        this.id = -1;
        this.cellArray = new CellArrayWritable();
    }

    public ROWCOLWritable(ROWCOLWritable rowcolWritable) {
        this.id = rowcolWritable.getId();
        this.cellArray = new CellArrayWritable(rowcolWritable.getCellArray());
    }

    public ROWCOLWritable(int id, CellArrayWritable cellArray) {

        this.id = id;
        this.cellArray = new CellArrayWritable(cellArray);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        cellArray.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readInt();
        cellArray.readFields(in);
    }
}
