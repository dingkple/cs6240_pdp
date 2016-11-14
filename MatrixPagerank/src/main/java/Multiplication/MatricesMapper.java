package Multiplication;

import Config.PagerankConfig;
import Preprocess.CellArrayWritable;
import Preprocess.CellWritable;
import Util.Utils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by kingkz on 11/12/16.
 */
public class MatricesMapper extends Mapper<IntWritable, Writable,
        IntWritable, ROWCOLWritable> {

    private long totalRecs;
    private int iter;
    private int blockNum;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        totalRecs = Long.valueOf(
                Utils.readData(PagerankConfig.NUMBER_OF_LINKS, context
                        .getConfiguration())
        );

        iter = Integer.valueOf(context.getConfiguration().get(PagerankConfig
                .ITER_NUM));

        blockNum = context.getConfiguration().getInt(String.valueOf(PagerankConfig
                .ROWCOL_BLOCK_SIZE_STRING), -1);
    }

    @Override
    protected void map(IntWritable key, Writable value,
                       Context context) {
        try {
            if (value instanceof CellArrayWritable) {
                context.write(
                        new IntWritable(key.get() % blockNum),
                        new ROWCOLWritable(key.get(), (CellArrayWritable) value)
                );
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

    }
}
