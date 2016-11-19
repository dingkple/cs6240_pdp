package Preprocess;

import Config.PagerankConfig;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by kingkz on 11/11/16.
 */
public class GraphKeyWritable implements WritableComparable<GraphKeyWritable> {

    private String name;
    private int type;
    private int count;

    public GraphKeyWritable() {
        this.name = "";
        this.type = -1;
        this.count = -1;
    }

    public GraphKeyWritable(int type, String name, int count) {
        this.name = name;
        this.type = type;
        this.count = count;
    }

    public GraphKeyWritable(int type, String name) {
        this.name = name;
        this.type = type;
        this.count = -1;
    }

    public GraphKeyWritable(int type, int count) {
        this.count = count;
        this.type = type;
        this.name = "";
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(type);
        out.writeInt(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        name = in.readUTF();
        type = in.readInt();
        count = in.readInt();
    }

    private int getReduceKey(String name) {
        return Math.abs(name.hashCode() % PagerankConfig.NUMBER_OF_REDUCERS_INT);
    }

    @Override
    public int compareTo(GraphKeyWritable o) {
        if (!name.equals(o.getName())) {
            return name.compareTo(o.getName());
        } else if (count != o.count) {
            return count - o.count;
        } else {
            return type - o.type;
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof GraphKeyWritable) {
            GraphKeyWritable g = (GraphKeyWritable) obj;
            if (g.name.equals(this.getName()) && g.type == this.type &&
                    g.getCount() == this.getCount()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = this.name.hashCode();
        result = 31 * result + Long.hashCode(this.getCount());
        result = 31 * result + Long.hashCode(this.getType());
        return result;
    }

    @Override
    public String toString() {
        return this.getType() + " " + this.getCount() + " " + this.getName();
    }
}
