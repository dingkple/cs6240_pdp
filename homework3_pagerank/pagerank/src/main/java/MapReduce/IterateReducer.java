package MapReduce;

import Pagerank.Config;
import Pagerank.Utils;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by kingkz on 10/17/16.
 */
public class IterateReducer extends Reducer<LinkPoint, CombineWritable, LinkPoint, LinkPointArrayWritable> {

    private double entropy;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        entropy = 0;
    }


    /**
     * Calculate the new raw pagerank in this iteration, entropy and sum of DANGLING_NAME weights too.
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
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

        if (key.getLineName().equals(Config.DANGLING_NAME)) {
            // Write dangling links's weight to file
            Utils.writeData(
                    Utils.totalDanglingWeight,
                    String.valueOf(weightSum),
                    context.getConfiguration());
        } else if (key.getLineName().equals(Config.ENTROPY_NAME)) {
            Utils.writeData(
                    Utils.entropy + "_" + context.getConfiguration().get(Config.ITER_NUM),
                    String.valueOf(weightSum),
                    context.getConfiguration());
        } else {

            // Set pagerank weights
            key.setWeight(weightSum);
            if (node == null) {
                node = new LinkPointArrayWritable();
            }

            context.write(key, node);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Write entropy to file
        Utils.writeData(Utils.entropy, String.valueOf(-1 * entropy), context.getConfiguration());
    }
}
