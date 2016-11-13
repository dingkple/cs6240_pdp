package Preprocess;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by kingkz on 11/11/16.
 */
public class CellArrayWritable extends ArrayWritable {

    public CellArrayWritable(CellWritable[] cells) {
        super(CellWritable.class);
        set(cells);
    }

    public CellArrayWritable() {
        super(CellWritable.class);
        set(new Text[0]);
    }


    private void readIterable(Iterator<String> itr, int size) {
        Text[] links = new Text[size];
        int i = 0;
        while (itr.hasNext()) {
            Text l = new Text(itr.next());
            links[i++] = l;
        }
        set(links);
    }

    public CellArrayWritable(Set<String> lpArray) {
        super(CellWritable.class);
        readIterable(lpArray.iterator(), lpArray.size());
    }

}
