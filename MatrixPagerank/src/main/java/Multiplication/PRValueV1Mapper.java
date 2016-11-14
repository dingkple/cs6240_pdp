package Multiplication;

import Config.PagerankConfig;
import Preprocess.CellArrayWritable;
import Preprocess.CellWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by kingkz on 11/12/16.
 */
public class PRValueV1Mapper extends Mapper<IntWritable,
        DoubleWritable, IntWritable, ROWCOLWritable> {
    private int blockNum;
    private double lastDanglingSum;
    private Long numberOfLinks;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //todo read number of records, used for splitting matrix
        blockNum = context.getConfiguration().getInt(
                String.valueOf(PagerankConfig.ROWCOL_BLOCK_SIZE_LONG), -1);
        lastDanglingSum = context.getConfiguration().getDouble(PagerankConfig
                .DANGLING_NAME, 0);

        numberOfLinks = context.getConfiguration().getLong(PagerankConfig
                .NUMBER_OF_LINKS, 0);
    }

    @Override
    protected void map(IntWritable key, DoubleWritable value, Context context)
            throws IOException, InterruptedException {
        //todo do the group by col thing:
        CellWritable[] cellWritables = new CellWritable[]{new CellWritable(key.get(), value
                .get
                () + lastDanglingSum / numberOfLinks)};
        for (int i = 0; i < blockNum; i++) {
            context.write(
                    new IntWritable(i),
                    new ROWCOLWritable(PagerankConfig.PAGERANK_COL
                            .hashCode(), new CellArrayWritable(cellWritables))
            );
        }
    }
}
