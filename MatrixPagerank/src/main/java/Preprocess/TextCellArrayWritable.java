package Preprocess;

import com.google.common.collect.Iterables;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

/**
 * Created by kingkz on 11/15/16.
 */
public class TextCellArrayWritable extends ArrayWritable {
    public TextCellArrayWritable() {
        super(TextCellWritable.class);
        set(new TextCellWritable[0]);
    }


    public TextCellArrayWritable(Iterable<TextCellWritable> iterable) {
        super(TextCellWritable.class);
        set(Iterables.toArray(iterable, TextCellWritable.class));
    }

    public TextCellArrayWritable(TextCellWritable[] iterable) {
        super(TextCellWritable.class);
        set(iterable);
    }
}
