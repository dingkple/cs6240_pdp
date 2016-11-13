package Preprocess;

import Config.PagerankConfig;
import com.google.common.collect.Iterables;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by kingkz on 11/11/16.
 */
public class LinkNameMapReducer extends Reducer<GraphKeyWritable, TextArrayWritable,
        Writable, Writable> {

    private Counter linkCounter;
    private MultipleOutputs multipleOutput;
    private List<CellArrayWritable> names;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        linkCounter = context.getCounter(PagerankConfig.PagerankCounter.LINK_COUNTER);
        multipleOutput = new MultipleOutputs<>(context);
        names = new ArrayList<>();
    }

    @Override
    protected void reduce(GraphKeyWritable key, Iterable<TextArrayWritable> values,
                          Context context)
            throws IOException, InterruptedException {

        names.clear();
        for (TextArrayWritable list : values) {
            if (list.get().length > 0) {
                for (Writable w : list.get()) {
                    names.add(w.toString().hashCode());
                }
            }
        }

        TextArrayWritable outlinks = new TextArrayWritable(Iterables.toArray
                (names, Integer.class));

        multipleOutput.write(
                PagerankConfig.RAW_LINK_GRAPH,
                key,
                outlinks,
                PagerankConfig.RAW_LINK_GRAPH + "/"
        );

        if (key.getType() == PagerankConfig.OUTLINK_TYPE) {
            linkCounter.increment(1);
            multipleOutput.write(
                    PagerankConfig.OUTPUT_LINKMAP,
                    new Text(key.getName()),
                    new LongWritable(linkCounter.getValue()),
                    PagerankConfig.OUTPUT_LINKMAP + "/"
            );

            multipleOutput.write(
                    PagerankConfig.OUTPUT_PAGERANK,
                    new LongWritable(linkCounter.getValue()),
                    new DoubleWritable(0.0),
                    PagerankConfig.OUTPUT_PAGERANK + "/"
            );
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutput.close();
    }
}
