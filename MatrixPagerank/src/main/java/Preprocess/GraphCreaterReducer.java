package Preprocess;

import Config.PagerankConfig;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/**
 * Created by kingkz on 11/11/16.
 */
public class GraphCreaterReducer extends Reducer<GraphKeyWritable,
        CellArrayWritable, LongWritable, CellArrayWritable> {


    private MultipleOutputs mos;

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        mos = new MultipleOutputs<>(context);

    }

    @Override
    protected void reduce(GraphKeyWritable key,
                          Iterable<CellArrayWritable> values, Context context)
            throws IOException, InterruptedException {
        for (CellArrayWritable cellArrayWritable : values) {
            if (key.getType() == PagerankConfig.INLINK_TYPE) {
                mos.write(
                        PagerankConfig.OUTPUT_INLINKS_MAPPED,
                        new LongWritable(key.getRowcol()),
                        cellArrayWritable,
                        PagerankConfig.OUTPUT_INLINKS_MAPPED + "/"
                        );
            } else {
                mos.write(
                        PagerankConfig.OUTPUT_OUTLINKS_MAPPED,
                        new LongWritable(key.getRowcol()),
                        cellArrayWritable,
                        PagerankConfig.OUTPUT_OUTLINKS_MAPPED + "/"
                );

                if (cellArrayWritable.get().length == 0) {
                    mos.write(
                            PagerankConfig.OUTPUT_DANGLING,
                            new LongWritable(key.getRowcol()),
                            NullWritable.get(),
                            PagerankConfig.OUTPUT_DANGLING + "/"
                    );
                }
            }
        }
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        mos.close();
    }
}
