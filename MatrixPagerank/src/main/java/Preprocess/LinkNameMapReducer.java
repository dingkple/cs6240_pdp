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
public class LinkNameMapReducer extends Reducer<GraphKeyWritable,
        GraphKeyArrayWritable, Writable, Writable> {

    private Counter linkCounter;
    private MultipleOutputs multipleOutput;
    private List<TextCellWritable> names;
    private List<TextCellWritable> danglings;
    private int counter1;
    private List<TextCellWritable> emptyInlinks;
    private double counter2 = 0;
    private double counter3 = 0;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        linkCounter = context.getCounter(PagerankConfig.PagerankCounter.LINK_COUNTER);
        multipleOutput = new MultipleOutputs<>(context);
        names = new ArrayList<>();
        danglings = new ArrayList<>();
        emptyInlinks = new ArrayList<>();
        counter1 = 0;
        counter3 = 0;
    }

    @Override
    protected void reduce(GraphKeyWritable key, Iterable<GraphKeyArrayWritable> values,
                          Context context)
            throws IOException, InterruptedException {

        names.clear();
        for (GraphKeyArrayWritable list : values) {
            if (list.get().length > 0) {
                for (Writable w : list.get()) {
                    GraphKeyWritable t = (GraphKeyWritable) w;
                    TextCellWritable cell = new TextCellWritable(
                            t.getName() , 1.0 / t.getCount());

                    if (key.getType() == PagerankConfig.INLINK_TYPE)
                        counter2 += 1.0 / t.getCount();
                    if (key.getType() == PagerankConfig.OUTLINK_TYPE)
                        counter3 += 1.0/t.getCount();
                    names.add(cell);
                }
            }
        }

        TextCellArrayWritable outlinks = new TextCellArrayWritable(names);


        if (key.getType() == PagerankConfig.OUTLINK_TYPE) {

            if (outlinks.get().length > 0) {
                multipleOutput.write(
                        PagerankConfig.OUTPUT_OUTLINKS,
                        key,
                        outlinks,
                        PagerankConfig.OUTPUT_OUTLINKS + "/"
                );
            } else {
                danglings.add(new TextCellWritable(key.getName(), 1.0));
            }
        } else if (key.getType() == PagerankConfig.INLINK_TYPE) {
            if (outlinks.get().length > 0) {

                multipleOutput.write(
                        PagerankConfig.OUTPUT_INLINKS,
                        key,
                        outlinks,
                        PagerankConfig.OUTPUT_INLINKS + "/"
                );
                counter1 += 1;
            } else {
                emptyInlinks.add(new TextCellWritable(key.getName(),
                        0.0));
            }
        } else {
            linkCounter.increment(1);
            multipleOutput.write(
                    PagerankConfig.OUTPUT_LINKMAP,
                    new Text(key.getName()),
                    new IntWritable((int) linkCounter.getValue()),
                    PagerankConfig.OUTPUT_LINKMAP + "/"
            );

            multipleOutput.write(
                    PagerankConfig.OUTPUT_PAGERANK,
                    new IntWritable((int) linkCounter.getValue()),
                    new DoubleWritable(0.0),
                    PagerankConfig.OUTPUT_PAGERANK + "1/"
            );
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        TextCellWritable[] danglingArr = Iterables.toArray(danglings,
                TextCellWritable.class);

        TextCellWritable[] emptyInlinkArr = Iterables.toArray(emptyInlinks,
                TextCellWritable.class);
        multipleOutput.write(
                PagerankConfig.OUTPUT_OUTLINKS,
                new GraphKeyWritable(
                        PagerankConfig.OUTLINK_TYPE,
                        PagerankConfig.DANGLING_NAME),
                new TextCellArrayWritable(danglingArr),
                PagerankConfig.OUTPUT_OUTLINKS + "/"
        );

        multipleOutput.write(
                PagerankConfig.OUTPUT_INLINKS,
                new GraphKeyWritable(
                        PagerankConfig.INLINK_TYPE,
                        PagerankConfig.DANGLING_NAME),
                new TextCellArrayWritable(danglingArr),
                PagerankConfig.OUTPUT_INLINKS + "/"
        );

        multipleOutput.write(
                PagerankConfig.OUTPUT_OUTLINKS,
                new GraphKeyWritable(
                        PagerankConfig.OUTLINK_TYPE,
                        PagerankConfig.EMPTY_INLINKS),
                new TextCellArrayWritable(emptyInlinkArr),
                PagerankConfig.OUTPUT_OUTLINKS + "/"
        );

        multipleOutput.write(
                PagerankConfig.OUTPUT_INLINKS,
                new GraphKeyWritable(
                        PagerankConfig.INLINK_TYPE,
                        PagerankConfig.EMPTY_INLINKS),
                new TextCellArrayWritable(emptyInlinkArr),
                PagerankConfig.OUTPUT_INLINKS + "/"
        );

        System.out.println("counter1: " + counter1);

        multipleOutput.write(
                PagerankConfig.OUTPUT_LINKMAP,
                new Text(PagerankConfig.DANGLING_NAME),
                new IntWritable(PagerankConfig.DANGLING_NAME_INT),
                PagerankConfig.OUTPUT_LINKMAP + "/"
        );

        multipleOutput.write(
                PagerankConfig.OUTPUT_LINKMAP,
                new Text(PagerankConfig.EMPTY_INLINKS),
                new IntWritable(PagerankConfig.EMPTY_INLINKS_INT),
                PagerankConfig.OUTPUT_LINKMAP + "/"
        );

        multipleOutput.close();
    }


}
