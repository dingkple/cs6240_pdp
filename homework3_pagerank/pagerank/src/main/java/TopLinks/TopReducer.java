package TopLinks;

import Pagerank.Config;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeMap;

/**
 * Created by kingkz on 10/17/16.
 */
public class TopReducer extends Reducer<DoubleWritable, Text, Text, Text> {

    private static int limit;
    private static int counter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        counter = 0;
        limit = Config.TOP_NUMBER;
    }

    @Override
    protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text name : values) {
            counter += 1;
            if (counter <= limit) {
                context.write(new Text(key.toString()), name);
            }
        }
    }

}
