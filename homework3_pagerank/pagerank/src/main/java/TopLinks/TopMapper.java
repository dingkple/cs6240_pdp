package TopLinks;

import MapReduce.LinkPoint;
import MapReduce.LinkPointArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by kingkz on 10/17/16.
 */
public class TopMapper extends Mapper<LinkPoint, LinkPointArrayWritable, DoubleWritable, Text> {


    @Override
    protected void map(LinkPoint key, LinkPointArrayWritable value, Context context) throws IOException, InterruptedException {
        context.write(new DoubleWritable(key.getWeight()), new Text(key.getLineName()));
    }
}
