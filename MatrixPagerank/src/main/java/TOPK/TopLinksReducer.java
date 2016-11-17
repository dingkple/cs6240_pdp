package TOPK;

import Config.PagerankConfig;
import Util.Utils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.net.URI;
import java.util.*;

/**
 * Created by kingkz on 11/13/16.
 */

public class TopLinksReducer
        extends Reducer<DoubleWritable, IntWritable, DoubleWritable,
        Text> {

    private static TreeMap<Double, String> topMap;
    private Map<Integer, String> linkMap;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        topMap = new TreeMap<>();
        linkMap = readLinkMap(context);
    }


    private Map<Integer, String> readLinkMap(Context context) throws
            IOException {
        URI[] uriArray = context.getCacheFiles();

        String path = null;
        if (uriArray != null) {
            for (URI uri : uriArray) {
                if (uri.getPath().contains(PagerankConfig.OUTPUT_LINKMAP)) {
                    path = uri.getPath();
                    break;
                }
            }
        }

        if (path == null) {
            path = Utils.getPathInTemp(context.getConfiguration(),
                    PagerankConfig.OUTPUT_LINKMAP)
                    .toString();
        }

        SequenceFile.Reader reader = new SequenceFile.Reader(context
                .getConfiguration(), SequenceFile.Reader.file
                (new Path(path+"/-r-00000")));

        Map<Integer, String> map = new HashMap<>();
        while (true) {
            Text key = new Text();
            IntWritable value = new IntWritable();

            if (!reader.next(key, value)) {
                break;
            }
            map.put(value.get(), key.toString());
        }

        return map;
    }

    /**
     * Write the first 100
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(DoubleWritable key, Iterable<IntWritable> values,
                          Context context)
            throws IOException, InterruptedException {
//        double pagerank = 0;
//        String linkname = null;
//        int counter = 0;
//        for (PagerankCellWritable cell : values) {
//            if (cell.getName().length() > 0) {
//                linkname = cell.getName();
//                counter += 1;
//            } else if (cell.getPagerank() > 0) {
//                pagerank = cell.getPagerank();
//                counter += 1;
//            }
//
//        }
//        if (counter == 2) {
        for (IntWritable link : values) {
            topMap.put(key.get(), linkMap.get(link.get()));
            if (topMap.size() > 100) {
                topMap.remove(topMap.firstKey());
            }
        }
//        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        List<Double> values = new ArrayList<>();
        values.addAll(topMap.keySet());
        values.sort((o1, o2) -> o2.compareTo(o1));
        for (double weight : values) {
            context.write(new DoubleWritable(weight), new Text(topMap.get
                    (weight)));
        }
    }
}