package Preprocess;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by kingkz on 11/11/16.
 */
public class CellWritable implements Writable {


    private int rowcol;
    private double value;

    public int getRowcol() {
        return rowcol;
    }

    public void setRowcol(int rowcol) {
        this.rowcol = rowcol;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public CellWritable (int rowcol, double value) {
        this.rowcol = rowcol;
        this.value = value;
    }

    public CellWritable() {
        this.value = 0;
        this.rowcol = -1;
    }

    public CellWritable (CellWritable c) {
        this.value = c.getValue();
        this.rowcol = c.getRowcol();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(rowcol);
        out.writeDouble(value);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        rowcol = in.readInt();
        value = in.readDouble();
    }

    @Override
    public int hashCode() {
        int result = 0;
        result = result * 31 + Integer.hashCode(rowcol);
        result = result * 31 + Double.hashCode(value);
        return result;
    }
}
