package MapReduce;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Created by kingkz on 10/17/16.
 */
public class LinkPointArrayWritable extends ArrayWritable {
    public LinkPointArrayWritable() {
        super(LinkPoint.class);
        set(new LinkPoint[0]);
    }

    public LinkPointArrayWritable(Set<LinkPoint> lpArray) {
        super(LinkPoint.class);
        readIterable(lpArray.iterator(), lpArray.size());
    }

    private void readIterable(Iterator<LinkPoint> itr, int size) {
        LinkPoint[] links = new LinkPoint[size];
        int i = 0;
        while (itr.hasNext()) {
            LinkPoint l = new LinkPoint(itr.next());
            links[i++] = l;
        }
        set(links);
    }

    public LinkPointArrayWritable(List<LinkPoint> lpArray) {
        super(LinkPoint.class);
        readIterable(lpArray.iterator(), lpArray.size());
    }
}
