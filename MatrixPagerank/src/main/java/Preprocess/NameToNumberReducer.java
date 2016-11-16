package Preprocess;

import Config.PagerankConfig;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by kingkz on 11/15/16.
 */


public class NameToNumberReducer extends Reducer<GraphKeyWritable,
        CellArrayWritable, IntWritable, CellArrayWritable> {

    private MultipleOutputs mos;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        mos = new MultipleOutputs<>(context);
    }

    @Override
    protected void reduce(GraphKeyWritable key, Iterable<CellArrayWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<CellArrayWritable> iter = values.iterator();
        if (iter.hasNext()) {
            CellArrayWritable cellArrayWritable = iter.next();
            if (key.getType() == PagerankConfig.OUTLINK_TYPE) {
                mos.write(
                        PagerankConfig.OUTPUT_OUTLINKS_MAPPED,
                        new IntWritable(key.getCount()),
                        cellArrayWritable,
                        PagerankConfig.OUTPUT_INLINKS_MAPPED + "/"
                );
            } else {
                mos.write(
                        PagerankConfig.OUTPUT_INLINKS_MAPPED,
                        new IntWritable(key.getCount()),
                        cellArrayWritable,
                        PagerankConfig.OUTPUT_INLINKS_MAPPED + "/"
                );
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }
}
