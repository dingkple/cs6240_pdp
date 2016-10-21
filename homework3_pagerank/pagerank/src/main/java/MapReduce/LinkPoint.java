package MapReduce;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by kingkz on 10/17/16.
 */
public class LinkPoint implements WritableComparable<LinkPoint>{

    private String lineName;
    private double weight;

    public LinkPoint(LinkPoint p) {
        this.lineName = p.getLineName();
        this.weight = p.getWeight();
    }

    public LinkPoint() {
        this.weight = 0;
    }

    public LinkPoint(String name, double w, double c) {
        this.lineName = name;
        this.weight = w;
    }

    public String getLineName() {
        return lineName;
    }

    public void setLineName(String lineName) {
        this.lineName = lineName;
    }

    public double getWeight() {
        return weight;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    @Override
    public int hashCode() {
        int result = this.getLineName().hashCode();
        result = 31 * result + Double.hashCode(this.getWeight());
        return result;
    }

    @Override
    public String toString() {
        return String.format("%s %.8f", lineName, weight);
    }

    @Override
    public boolean equals(Object obj) {
        LinkPoint lp = (LinkPoint) obj;
        return this.toString().equals(lp.toString());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(lineName);
        out.writeDouble(weight);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        lineName = in.readUTF();
        weight = in.readDouble();
    }

    @Override
    public int compareTo(LinkPoint o) {
        if (this.getWeight() != o.getWeight()) {
            return (this.getWeight() < o.getWeight()) ? -1 : 1;
        } else {
            return this.getLineName().compareTo(o.getLineName());
        }
    }

    public void clear() {
        this.weight = 0;
    }
}
