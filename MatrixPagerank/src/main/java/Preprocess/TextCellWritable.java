package Preprocess;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by kingkz on 11/15/16.
 */

public class TextCellWritable implements Writable {


    private String rowcol;
    private double value;

    public String getRowcol() {
        return rowcol;
    }

    public void setRowcol(String rowcol) {
        this.rowcol = rowcol;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public TextCellWritable (String rowcol, double value) {
        this.rowcol = rowcol;
        this.value = value;
    }

    public TextCellWritable() {
        this.value = 0;
        this.rowcol = "";
    }

    public TextCellWritable (TextCellWritable c) {
        this.value = c.getValue();
        this.rowcol = c.getRowcol();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(rowcol);
        out.writeDouble(value);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        rowcol = in.readUTF();
        value = in.readDouble();
    }

    @Override
    public int hashCode() {
        int result = 0;
        result = result * 31 + rowcol.hashCode();
        result = result * 31 + Double.hashCode(value);
        return result;
    }
}
