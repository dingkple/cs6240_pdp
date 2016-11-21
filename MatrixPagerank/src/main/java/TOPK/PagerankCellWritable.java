package TOPK;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by kingkz on 11/13/16.
 */
public class PagerankCellWritable implements Writable {

    String name;
    double pagerank;

    public PagerankCellWritable(String name, double pagerank) {
        this.name = name;
        this.pagerank = pagerank;
    }

    public PagerankCellWritable() {
        this.name = "";
        this.pagerank = 0;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getPagerank() {
        return pagerank;
    }

    public void setPagerank(double pagerank) {
        this.pagerank = pagerank;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeDouble(pagerank);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        name = in.readUTF();
        pagerank = in.readDouble();
    }
}
