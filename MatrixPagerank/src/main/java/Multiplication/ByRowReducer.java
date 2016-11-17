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

        URI path = null;
        if (uriArray != null) {
            for (URI uri : uriArray) {
                System.out.println("current checking uri: " + uri.toString());
                System.out.println("pagerankdir: " + PagerankConfig
                        .OUTPUT_PAGERANK + iterNumber);
                System.out.println(uri.toString().contains(PagerankConfig
                        .OUTPUT_PAGERANK + iterNumber));
                if (uri.toString().contains(PagerankConfig
                        .OUTPUT_PAGERANK + iterNumber)) {
                    path = uri;
                    System.out.println("Im not NULL: !!!" + path);
                    break;
                }
            }
        }

        if (path == null) {
            String pathStr = PagerankConfig
                    .OUTPUT_PAGERANK +
                    iterNumber;
            if (iterNumber == 1) {
                pathStr += "/-r-00000";
            } else {
                pathStr += "/part-r-00000";
            }
            path = Utils.getPathInTemp(context.getConfiguration(), pathStr)
                    .toUri();
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

//        if (key.get() == PagerankConfig.DANGLING_NAME_INT) {
//            Utils.writeData(PagerankConfig.DANGLING_FILENAME, String.valueOf
//                    (v), context.getConfiguration());
//        } else {
            pagerankMap.put(key.get(), v);
//        }

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

        Utils.writeData(PagerankConfig.DANGLING_FILENAME, String.valueOf(1 -
                counter), context.getConfiguration());
        System.out.println(counter);
    }
}
