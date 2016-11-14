package Preprocess;

import Config.PagerankConfig;
import com.google.common.collect.Iterables;
import org.apache.hadoop.io.*;
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
    private List<CellWritable> names;
    private List<CellWritable> danglings;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        linkCounter = context.getCounter(PagerankConfig.PagerankCounter.LINK_COUNTER);
        multipleOutput = new MultipleOutputs<>(context);
        names = new ArrayList<>();
        danglings = new ArrayList<>();
    }

    @Override
    protected void reduce(GraphKeyWritable key, Iterable<TextArrayWritable> values,
                          Context context)
            throws IOException, InterruptedException {

        names.clear();
        for (TextArrayWritable list : values) {
            if (list.get().length > 0) {
                for (Writable w : list.get()) {
                    Text t = (Text) w;
                    CellWritable cell = new CellWritable(
                            t.toString().hashCode() , 0.0);
                    names.add(cell);
                }
            }
        }

        if (names.size() > 0) {
            for (CellWritable cell : names) {
                cell.setValue(1.0 / names.size());
            }
        } else {
            danglings.add(new CellWritable(key.getName().hashCode(), 1.0));
        }

        CellArrayWritable outlinks = new CellArrayWritable(Iterables.toArray
                (names, CellWritable.class));


        if (key.getType() == PagerankConfig.OUTLINK_TYPE) {
            linkCounter.increment(1);

            if (outlinks.get().length > 0) {
                multipleOutput.write(
                        PagerankConfig.OUTPUT_OUTLINKS,
                        new IntWritable(key.getName().hashCode()),
                        outlinks,
                        PagerankConfig.OUTPUT_OUTLINKS + "/"
                );
            }

            multipleOutput.write(
                    PagerankConfig.OUTPUT_LINKMAP,
                    new Text(key.getName()),
                    new IntWritable(key.getName().hashCode()),
                    PagerankConfig.OUTPUT_LINKMAP + "/"
            );

            multipleOutput.write(
                    PagerankConfig.OUTPUT_PAGERANK,
                    new IntWritable(key.getName().hashCode()),
                    new DoubleWritable(0.0),
                    PagerankConfig.OUTPUT_PAGERANK + "1/"
            );
        } else {
            if (outlinks.get().length > 0) {
                multipleOutput.write(
                        PagerankConfig.OUTPUT_INLINKS,
                        new IntWritable(key.getName().hashCode()),
                        outlinks,
                        PagerankConfig.OUTPUT_INLINKS + "/"
                );
            }
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        CellWritable[] danglingArr = Iterables.toArray(danglings,
                CellWritable.class);
        multipleOutput.write(
                PagerankConfig.OUTPUT_OUTLINKS,
                new IntWritable(PagerankConfig.DANGLING_NAME.hashCode()),
                new CellArrayWritable(danglingArr),
                PagerankConfig.OUTPUT_OUTLINKS + "/"
        );

        multipleOutput.write(
                PagerankConfig.OUTPUT_INLINKS,
                new IntWritable(PagerankConfig.DANGLING_NAME.hashCode()),
                new CellArrayWritable(danglingArr),
                PagerankConfig.OUTPUT_INLINKS + "/"
        );

        multipleOutput.close();
    }
}
