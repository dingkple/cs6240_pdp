package MapReduce;

import Pagerank.Config;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by kingkz on 10/19/16.
 */
public class IteratePartitioner extends Partitioner<LinkPoint, CombineWritable> {
    @Override
    public int getPartition(LinkPoint linkPoint, CombineWritable combineWritable, int numPartitions) {
        if (linkPoint.getLineName().equals(Config.DANGLING_NAME)) {
            return Math.abs(Config.DANGLING_NAME.hashCode()) % numPartitions;
        }

        if (linkPoint.getLineName().equals(Config.ENTROPY_NAME)) {
            return Math.abs(Config.ENTROPY_NAME.hashCode()) % numPartitions;
        }

        return Math.abs(linkPoint.hashCode()) % numPartitions;
    }
}
