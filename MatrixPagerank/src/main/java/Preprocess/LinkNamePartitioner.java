package Preprocess;

import Config.PagerankConfig;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by kingkz on 11/15/16.
 */
public class LinkNamePartitioner
        extends Partitioner<GraphKeyWritable, GraphKeyArrayWritable> {


    @Override
    public int getPartition(GraphKeyWritable graphKeyWritable,
                            GraphKeyArrayWritable graphKeyArrayWritable,
                            int numPartitions) {
        if (numPartitions < 2) {
            return 0;
        }
        if (graphKeyWritable.getType() == PagerankConfig.LINK_MAP_TYPE) {
            return 0;
        } else {
        return 1 +
                Math.abs(
                graphKeyWritable.getName().hashCode() %
                        (numPartitions - 1)
                );
        }
    }
}
