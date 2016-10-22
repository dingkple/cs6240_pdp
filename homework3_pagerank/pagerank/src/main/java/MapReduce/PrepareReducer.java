package MapReduce;

import Pagerank.RunPagerank;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

/**
 * Created by kingkz on 10/21/16.
 */
public class PrepareReducer extends Reducer<LinkPoint, LinkPointArrayWritable, LinkPoint, LinkPointArrayWritable> {

    private Counter danglingCounter;
    private Counter linkCounter;

    private LinkPointArrayWritable emptyOutlinks;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        danglingCounter = context.getCounter(RunPagerank.UpdateCounter.NUMBER_OF_DANGLING);
        linkCounter = context.getCounter(RunPagerank.UpdateCounter.NUMBER_OF_RECORD);
        emptyOutlinks = new LinkPointArrayWritable();
    }

    @Override
    protected void reduce(LinkPoint key, Iterable<LinkPointArrayWritable> values, Context context) throws IOException, InterruptedException {
        HashSet<LinkPoint> outlinks = new HashSet<>();

        linkCounter.increment(1);
        for (LinkPointArrayWritable w : values) {
            for (Writable o : w.get()) {
                outlinks.add(new LinkPoint((LinkPoint)o));
            }
        }

        if (outlinks.size() == 0) {
            danglingCounter.increment(1);
            context.write(key, emptyOutlinks);
        } else {
            context.write(key, new LinkPointArrayWritable(outlinks));
        }
    }
}
