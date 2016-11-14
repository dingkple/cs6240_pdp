package TOPK;

import Config.PagerankConfig;
import Util.Utils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by kingkz on 11/13/16.
 */
public class PagerankValueMapper extends Mapper <IntWritable, DoubleWritable,
        IntWritable, PagerankCellWritable> {


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

    }

    @Override
    protected void map(IntWritable key, DoubleWritable value, Context
            context) throws IOException, InterruptedException {
        // Note that we don't need to add the dangling value here, since each
        // link will get D / V, and this won't affect the final order at all.
//        context.write(
//                new IntWritable(key.get()),
//                new PagerankCellWritable("", value.get() +
//                        danglingSum/numberOfLinks));
    }
}
