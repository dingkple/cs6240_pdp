package MapReduce;

import Pagerank.Config;
import Pagerank.Utils;
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

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        lastTotalWeight = Double.valueOf(Utils.readData(Utils.totalWeight, context.getConfiguration()));
        numberOfRecs = Long.valueOf(Utils.readData(Utils.numberOfRecords, context.getConfiguration()));
        totalSink = Double.valueOf(Utils.readData(Utils.totalSink, context.getConfiguration()));
        list = new ArrayList<>();
        entropy = 0;
        totalWeight = 0;
        d1 = 0;
        d2 = 0;
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
        d1 += weightSum;
        double alpha = Config.PAGERANK_D;

        weightSum = alpha * (weightSum + totalSink / numberOfRecs) + (1 - alpha) / numberOfRecs;
        weightSum /= lastTotalWeight;
        key.setWeight(weightSum);
        entropy += weightSum * Math.log(weightSum) / Math.log(2);
        if (node == null) {
            node = new LinkPointArrayWritable(new ArrayList<>());
        }

        totalWeight += weightSum;
        if (node != null) {
            context.write(key, node);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.format("total weight: %f, entropy: %f ", totalWeight, entropy);
        Utils.writeData(Utils.entropy, String.valueOf(-1 * entropy), context.getConfiguration());
        Utils.writeData(Utils.totalWeight, String.valueOf(totalWeight), context.getConfiguration());
//        Utils.writeData();
    }
}
