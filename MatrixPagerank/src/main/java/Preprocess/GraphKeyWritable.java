package Preprocess;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by kingkz on 11/11/16.
 */
public class GraphKeyWritable implements WritableComparable<GraphKeyWritable> {

    private String name;
    private long type;
    private long rowcol;

    public GraphKeyWritable() {
        this.name = "";
        this.type = -1;
        this.rowcol = -1;
    }

    public GraphKeyWritable(long type, String name, long rowcol) {
        this.name = name;
        this.type = type;
        this.rowcol = rowcol;
    }

    public GraphKeyWritable(long type, String name) {
        this.name = name;
        this.type = type;
        this.rowcol = -1;
    }

    public GraphKeyWritable(long type, long rowcol) {
        this.rowcol = rowcol;
        this.type = type;
        this.name = "";
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getType() {
        return type;
    }

    public void setType(long type) {
        this.type = type;
    }

    public long getRowcol() {
        return rowcol;
    }

    public void setRowcol(long rowcol) {
        this.rowcol = rowcol;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeLong(type);
        out.writeLong(rowcol);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        name = in.readUTF();
        type = in.readLong();
        rowcol = in.readLong();
    }

    @Override
    public int compareTo(GraphKeyWritable o) {
        if (rowcol != o.rowcol) {
            return (int) (rowcol - o.rowcol);
        } else if (type != o.type) {
            return (int) (type - o.type);
        } else {
            return name.compareTo(o.name);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof GraphKeyWritable) {
            GraphKeyWritable g = (GraphKeyWritable) obj;
            if (g.name.equals(this.getName()) && g.type == this.type &&
                    g.getRowcol() == this.getRowcol()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = this.name.hashCode();
        result = 31 * result + Long.hashCode(this.getRowcol());
        result = 31 * result + Long.hashCode(this.getType());
        return result;
    }

    @Override
    public String toString() {
        return this.getType() + " " + this.getRowcol() + " " + this.getName();
    }
}
