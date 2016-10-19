package MapReduce;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

import java.util.ArrayList;

/**
 * Created by kingkz on 10/17/16.
 */
public class LinkPointArrayWritable extends ArrayWritable {
    public LinkPointArrayWritable() {
        super(LinkPoint.class);
        set(new LinkPoint[0]);
    }


    public LinkPointArrayWritable(ArrayList<LinkPoint> lpArray) {
        super(LinkPoint.class);
        LinkPoint[] points = new LinkPoint[lpArray.size()];
        for (int i = 0; i < points.length; i++) {
            points[i] = new LinkPoint(lpArray.get(i));
        }
        set(points);
    }
}
