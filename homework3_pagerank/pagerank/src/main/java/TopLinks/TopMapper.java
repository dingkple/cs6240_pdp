package TopLinks;

import MapReduce.LinkPoint;
import MapReduce.LinkPointArrayWritable;
import Pagerank.Config;
import Pagerank.Utils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.TreeMap;


/**
 * Created by kingkz on 10/17/16.
 */
public class TopMapper extends Mapper<LinkPoint, LinkPointArrayWritable, DoubleWritable, Text> {

    private TreeMap<Double, String> topMap  ;
    private double numberOfRecs;
    private double totalSink;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        topMap = new TreeMap<>();
        numberOfRecs = Long.valueOf(context.getConfiguration().get(Utils.numberOfRecords));
        totalSink = Double.valueOf(Utils.readData(Utils.totalDanglingWeight, context.getConfiguration()));
    }

    /**
     * Read the key/value pairs from last iteration, and calculate the real pagerank, then put them in a treeMap, which
     * discards the smallest pagerank link when its size exceeds the limit
     * @param key A linkPoint
     * @param value Its outlinks, not used here
     * @param context Context of mapper
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LinkPoint key, LinkPointArrayWritable value, Context context)
            throws IOException, InterruptedException {
        double alpha = Config.PAGERANK_D;
        double keyWeight = key.getWeight();
        double p1 = alpha * (keyWeight + totalSink / numberOfRecs);
        double p2 = (1 - alpha) / numberOfRecs;
        keyWeight = p1 + p2;

        topMap.put(keyWeight, key.getLineName());
        if (topMap.size() > Config.TOP_NUMBER) {
            topMap.remove(topMap.firstKey());
        }
    }

    /**
     * Output the top 100 links in the mapper
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Double weight : topMap.descendingKeySet()) {
            context.write(new DoubleWritable(weight), new Text(topMap.get(weight)));
        }
    }
}
