package Multiplication;

import Config.PagerankConfig;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by kingkz on 11/15/16.
 */
public class MultiplicationPartitioner extends Partitioner<IntWritable, ROWCOLArrayWritable> {
    @Override
    public int getPartition(IntWritable key, ROWCOLArrayWritable value, int numPartitions) {
        int reduerId;
        if (key.get() == 0) {
            reduerId = 0;
        } else {
            reduerId = Math.abs(key.get() % (numPartitions-1)) + 1;
        }
        return reduerId;
    }
}
