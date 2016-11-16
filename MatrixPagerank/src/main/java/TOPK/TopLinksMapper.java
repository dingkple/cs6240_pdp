package TOPK;

import Config.PagerankConfig;
import Util.Utils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Comparator;
import java.util.TreeMap;

/**
 * Created by kingkz on 11/13/16.
 */
public class TopLinksMapper extends Mapper<IntWritable, DoubleWritable,
        DoubleWritable, IntWritable> {

    private TreeMap<Double, Integer> topMap;

    private double danglingSum;
    private long numberOfLinks;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        topMap = new TreeMap<>();

        if (context.getConfiguration().get(PagerankConfig.DANGLING_NAME)==null) {
            danglingSum = Double.parseDouble(
                    Utils.readData(PagerankConfig.DANGLING_FILENAME,
                            context.getConfiguration()));
        } else {
            danglingSum = context.getConfiguration().getDouble(PagerankConfig
                    .DANGLING_NAME, 0.0);
        }

        if (context.getConfiguration().get(PagerankConfig.DANGLING_NAME) ==
                null) {
            numberOfLinks = Long.parseLong(Utils.readData(PagerankConfig.NUMBER_OF_LINKS,
                    context.getConfiguration()));
        } else {
            numberOfLinks = context.getConfiguration().getLong(PagerankConfig
                    .NUMBER_OF_LINKS, 0);
        }
    }

    @Override
    protected void map(IntWritable key, DoubleWritable value, Context context)
            throws IOException, InterruptedException {
        topMap.put((value.get() + danglingSum / numberOfLinks) * 0.85 + 0.15/numberOfLinks,
                key.get());
        if (topMap.size() > 100) {
            topMap.remove(topMap.firstKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Double weight : topMap.descendingKeySet()) {
            context.write(
                    new DoubleWritable(weight),
                    new IntWritable(topMap.get(weight)));
        }
    }
}
