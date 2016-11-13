package Multiplication;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by kingkz on 11/12/16.
 */
public class MultiplicationReducer extends Reducer<LongWritable, Writable,
        LongWritable, DoubleWritable> {

    @Override
    protected void reduce(LongWritable key, Iterable<Writable> values,
                          Context context) throws IOException,
            InterruptedException {

    }
}
