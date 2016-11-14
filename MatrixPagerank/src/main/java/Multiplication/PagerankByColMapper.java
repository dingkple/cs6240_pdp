package Multiplication;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by kingkz on 11/14/16.
 */
public class PagerankByColMapper extends Mapper<IntWritable, DoubleWritable,
        IntWritable, DoubleWritable> {


    private Map<Integer, Double> valueMap;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        valueMap = new HashMap<>();
    }

    @Override
    protected void map(IntWritable key, DoubleWritable value, Context context) throws IOException, InterruptedException {
        if (!valueMap.containsKey(key.get())) {
            valueMap.put(key.get(), 0.0);
        }
        valueMap.put(key.get(), valueMap.get(key.get()) + value.get());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (int key : valueMap.keySet()) {
            context.write(
                    new IntWritable(key),
                    new DoubleWritable(valueMap.get(key))
            );
        }
    }
}
