package Multiplication;

import Config.PagerankConfig;
import Util.Utils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by kingkz on 11/15/16.
 */
public class ByRowReducer extends Reducer<IntWritable, DoubleWritable,
        IntWritable, DoubleWritable> {

    private double counter;
    private int iterNumber;
    private int numberOfLinks;
    private double lastDanglingSum;
    private Map<Integer, Double> pagerankMap;


    private Map<Integer, Double> readPagerankValue(Context context) throws
            IOException {
        URI[] uriArray = context.getCacheFiles();

        String path = null;
        if (uriArray != null) {
            for (URI uri : uriArray) {
                if (uri.getPath().contains(PagerankConfig
                        .OUTPUT_PAGERANK + iterNumber)) {
                    path = uri.getPath();
                    break;
                }
            }
        }

        if (path == null) {
            path = Utils.getPathInTemp(PagerankConfig.OUTPUT_PAGERANK +
                    iterNumber)
                    .toString();
        }

        if (iterNumber == 1) {
            path += "/-r-00000";
        } else {
            path += "/part-r-00000";
        }

        SequenceFile.Reader reader = new SequenceFile.Reader(context
                .getConfiguration(), SequenceFile.Reader.file
                (new Path(path)));

        Map<Integer, Double> map = new HashMap<>();
        while (true) {
            IntWritable key = new IntWritable();
            DoubleWritable value = new DoubleWritable();

            if (!reader.next(key, value)) {
                break;
            }
            map.put(key.get(), 0.0);
        }

        return map;
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        counter = 0.0;

        iterNumber = Integer.valueOf(context.getConfiguration().get(PagerankConfig
                .ITER_NUM));

        counter = 0.0;


        lastDanglingSum = context.getConfiguration().getDouble(PagerankConfig
                .DANGLING_NAME, 0);

        pagerankMap = readPagerankValue(context);
    }

    @Override
    protected void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double v = 0;
        for (DoubleWritable d : values) {
            v += d.get();
        }

        if (key.get() == PagerankConfig.DANGLING_NAME_INT) {
            Utils.writeData(PagerankConfig.DANGLING_FILENAME, String.valueOf
                    (v), context.getConfiguration());
        } else {
            pagerankMap.put(key.get(), v);
        }

        counter += v;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        IntWritable key = new IntWritable();
        DoubleWritable value = new DoubleWritable();
        for (int k : pagerankMap.keySet()) {
            key.set(k);
            value.set(pagerankMap.get(k));
            context.write(
                    key,
                    value
            );
        }
        System.out.println(counter);
    }
}
