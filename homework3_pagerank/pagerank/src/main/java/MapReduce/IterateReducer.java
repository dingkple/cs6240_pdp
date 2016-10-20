package MapReduce;

import Pagerank.Config;
import Pagerank.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by kingkz on 10/17/16.
 */
public class IterateReducer extends Reducer<LinkPoint, CombineWritable, LinkPoint, LinkPointArrayWritable> {

    private ArrayList<LinkPoint> list;

    private double totalWeight;
    private long numberOfRecs;
    private double entropy;
    private double totalSink;
    private Double lastTotalWeight;

    private double d1, d2;
    private double numberOfSink;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        numberOfRecs = Long.valueOf(context.getConfiguration().get(Utils.numberOfRecords));
        totalSink = readSink(context.getConfiguration());
        list = new ArrayList<>();
        entropy = 0;
        totalWeight = 0;
        d1 = 0;
        d2 = 0;
    }

    private double readSink(Configuration conf) throws IOException {
        double sink = Double.valueOf(Utils.readData(Utils.totalSink, conf));
        if (sink == 0) {
            sink = 1. / numberOfRecs * numberOfSink;
        }
        return sink;
    }


    @Override
    protected void reduce(LinkPoint key, Iterable<CombineWritable> values, Context context) throws IOException, InterruptedException {
        double weightSum = 0;
        LinkPointArrayWritable node = null;
        for (CombineWritable cb : values) {
            if (cb.diff != 0) {
                weightSum += cb.diff;
            } else {
                node = cb.getLinkPointArray();
            }

        }

        if (key.getLineName().equals(Config.SINK_NAME)) {
            Utils.writeData(
                    Utils.totalSink,
                    String.valueOf(weightSum),
                    context.getConfiguration());
            totalWeight += weightSum;
        } else if (key.getLineName().equals(Config.ENTROPY_NAME)) {
            Utils.writeData(
                    Utils.entropy + "_" + context.getConfiguration().get(Config.ITER_NUM),
                    String.valueOf(weightSum),
                    context.getConfiguration());
        } else {

            d1 += weightSum;
            double alpha = Config.PAGERANK_D;

            weightSum = alpha * (weightSum + totalSink / numberOfRecs) + (1 - alpha) / numberOfRecs;
//            weightSum /= lastTotalWeight;
            key.setWeight(weightSum);
//            entropy += weightSum * Math.log(weightSum) / Math.log(2);
            if (node == null) {
                node = new LinkPointArrayWritable();
            }

            totalWeight += weightSum;
            context.write(key, node);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
//        System.out.format("total weight: %f, entropy: %f ", totalWeight, entropy);
        Utils.writeData(Utils.entropy, String.valueOf(-1 * entropy), context.getConfiguration());
//        Utils.writeData(Utils.totalWeight, String.valueOf(totalWeight), context.getConfiguration());
////        Utils.writeData();
    }
}
