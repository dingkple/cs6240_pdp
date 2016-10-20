package MapReduce;

import Pagerank.Config;
import Pagerank.Utils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by kingkz on 10/17/16.
 */
public class IterateMapper extends Mapper<LinkPoint, LinkPointArrayWritable, LinkPoint, CombineWritable> {


    private static long recordNum;
    private static double oriWeight;
    private LinkPoint sink;
    private double totalSink;
    private double entropy;
    private LinkPoint dummyEntropy;
    private int c1, c2;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        recordNum = Long.valueOf(context.getConfiguration().get(Utils.numberOfRecords));

        oriWeight = 1./ recordNum;

        sink = new LinkPoint(Config.SINK_NAME, 0, 0);
        dummyEntropy = new LinkPoint(Config.ENTROPY_NAME, 0, 0);

        totalSink = 0;
        c1 = 0;
        c2 = 0;
    }

    // TODO: 10/17/16 generate value change for every outlink
    @Override
    protected void map(LinkPoint key, LinkPointArrayWritable value, Context context) throws IOException, InterruptedException {
        Writable[] array = value.get();

        c1 += 1;
        double keyWeight;
        if (key.getWeight() == 0) {
            keyWeight = oriWeight;
        } else {
            keyWeight = key.getWeight();
        }

        entropy += keyWeight * Math.log(keyWeight) / Math.log(2);

        CombineWritable cb = new CombineWritable(value, 0);

        LinkPoint carpKey = new LinkPoint(key);
        carpKey.clear();

        context.write(carpKey, cb);

        double change = 0;

        if (array.length > 0) {
            change = keyWeight / array.length;
        } else {
            totalSink += keyWeight;
            c2 += 1;
        }
        for (Writable lp : array) {
            LinkPoint l = (LinkPoint) lp;
            l.clear();
            context.write(l, new CombineWritable(change));
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(dummyEntropy, new CombineWritable(entropy));
        context.write(sink, new CombineWritable(totalSink));
    }
}
