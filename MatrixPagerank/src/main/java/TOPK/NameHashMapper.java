package TOPK;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by kingkz on 11/13/16.
 */
public class NameHashMapper extends Mapper<Text, IntWritable, IntWritable,
        PagerankCellWritable> {

    @Override
    protected void map(Text key, IntWritable value, Context context)
            throws IOException, InterruptedException {
        context.write(
                new IntWritable(value.get()),
                new PagerankCellWritable(key.toString(), -1));
    }
}
