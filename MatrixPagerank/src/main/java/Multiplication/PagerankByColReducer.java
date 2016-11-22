package Multiplication;

import Config.PagerankConfig;
import Util.Utils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by kingkz on 11/14/16.
 */
public class PagerankByColReducer extends Reducer<IntWritable,
        DoubleWritable, IntWritable, DoubleWritable> {

    private double counter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        counter = 0.0;
    }

    @Override
    protected void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double v = 0;
        for (Writable w : values) {
            DoubleWritable dw = (DoubleWritable) w;
            v += dw.get();
        }
        if (key.get() == PagerankConfig.DANGLING_NAME_INT {
            Utils.writeData(
                    PagerankConfig.DANGLING_FILENAME,
                    String.valueOf(v),
                    context.getConfiguration()
            );
            counter += v;
        } else {
            counter += v;
            context.write(
                    new IntWritable(key.get()),
                    new DoubleWritable(v)
            );
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println(counter);
    }
}
