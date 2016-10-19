package MapReduce;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by kingkz on 10/18/16.
 */
public class CombineWritable implements Writable {
    LinkPointArrayWritable pointList;

    double diff;

    public CombineWritable() {
        this.pointList = new LinkPointArrayWritable();
        this.diff = 0;
    }

    public CombineWritable(LinkPointArrayWritable list, double diff) {
        this.diff = diff;

        ArrayList<LinkPoint> listNew = new ArrayList<>();
        if (list != null) {
            for (Writable w : list.get()) {
                LinkPoint newL = new LinkPoint((LinkPoint)w);
                listNew.add(newL);
            }
        }
        this.pointList = new LinkPointArrayWritable(listNew);
    }



    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(diff);
        pointList.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        diff = in.readDouble();
        pointList.readFields(in);
    }

    public LinkPointArrayWritable getLinkPointArray() {
        ArrayList<LinkPoint> listNew = new ArrayList<>();
        for (Writable w : pointList.get()) {
            LinkPoint newL = new LinkPoint((LinkPoint)w);
            listNew.add(newL);
        }

        return new LinkPointArrayWritable(listNew);
    }
}
