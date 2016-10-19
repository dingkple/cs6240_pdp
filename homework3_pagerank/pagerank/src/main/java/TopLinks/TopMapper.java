package TopLinks;

import MapReduce.LinkPoint;
import MapReduce.LinkPointArrayWritable;
import Pagerank.Config;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.TreeMap;

/**
 * Created by kingkz on 10/17/16.
 */
public class TopMapper extends Mapper<LinkPoint, LinkPointArrayWritable, DoubleWritable, Text> {

    private TreeMap<Double, String> topMap  ;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        topMap = new TreeMap<>();
    }

    @Override
    protected void map(LinkPoint key, LinkPointArrayWritable value, Context context) throws IOException, InterruptedException {

        topMap.put(key.getWeight(), key.getLineName());
        if (topMap.size() > Config.TOP_NUMBER) {
            topMap.remove(topMap.firstKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Double weight : topMap.descendingKeySet()) {
            context.write(new DoubleWritable(weight), new Text(topMap.get(weight)));
        }
    }
}
