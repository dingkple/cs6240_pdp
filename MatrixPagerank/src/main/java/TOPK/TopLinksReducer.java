package TOPK;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeMap;

/**
 * Created by kingkz on 11/13/16.
 */

public class TopLinksReducer
        extends Reducer<IntWritable, PagerankCellWritable, DoubleWritable,
        Text> {

    private static TreeMap<Double, String> topMap;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        topMap = new TreeMap<>();
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
    protected void reduce(IntWritable key, Iterable<PagerankCellWritable> values,
                          Context context)
            throws IOException, InterruptedException {
        double pagerank = 0;
        String linkname = null;
        int counter = 0;
        for (PagerankCellWritable cell : values) {
            if (cell.getName().length() > 0) {
                linkname = cell.getName();
            } else {
                pagerank = cell.getPagerank();
            }
            counter += 1;
        }
        if (counter == 2) {
            topMap.put(pagerank, linkname);
            if (topMap.size() > 100) {
                topMap.remove(topMap.firstKey());
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (double weight : topMap.keySet()) {
            context.write(new DoubleWritable(weight), new Text(topMap.get
                    (weight)));
        }
    }
}