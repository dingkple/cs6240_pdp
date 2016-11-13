package Preprocess;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Created by kingkz on 11/11/16.
 */
public class TextArrayWritable extends ArrayWritable {
    public TextArrayWritable(Text[] strings) {
        super(Text.class);
        set(strings);
    }

    public TextArrayWritable() {
        super(Text.class);
        set(new Text[0]);
    }

    public TextArrayWritable (Text t) {
        super (Text.class);
        set(new Text[]{t});
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

    public TextArrayWritable(Set<String> lpArray) {
        super(Text.class);
        readIterable(lpArray.iterator(), lpArray.size());
    }
}
