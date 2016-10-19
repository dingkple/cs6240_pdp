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

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        recordNum = Long.valueOf(Utils.readData(Utils.numberOfRecords, context.getConfiguration()));

        oriWeight = 1./recordNum;

        sink = new LinkPoint(Config.SINK_NAME, 0, 0);

        totalSink = 0;
    }

    // TODO: 10/17/16 generate value change for every outlink
    @Override
    protected void map(LinkPoint key, LinkPointArrayWritable value, Context context) throws IOException, InterruptedException {
        Writable[] array = value.get();

        double keyWeight;
        if (key.getWeight() == 0) {
            keyWeight = oriWeight;
        } else {
            keyWeight = key.getWeight();
        }

        CombineWritable cb = new CombineWritable(value, 0);

        LinkPoint carpKey = new LinkPoint(key);
        carpKey.clear();

        context.write(carpKey, cb);

        double change;

        if (array.length == 0) {
            totalSink += keyWeight;
            change = 0;
        } else {
            change = keyWeight / array.length;
        }
        for (Writable lp : array) {
            LinkPoint l = (LinkPoint) lp;
            l.setWeight(0);
            l.setChange(0);
            context.write(l, new CombineWritable(new LinkPointArrayWritable(), change));
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Utils.writeData(Utils.totalSink, String.valueOf(totalSink), context.getConfiguration());
    }
}
