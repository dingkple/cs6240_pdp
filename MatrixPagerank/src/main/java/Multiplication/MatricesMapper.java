package Multiplication;

import Config.PagerankConfig;
import Preprocess.CellArrayWritable;
import Util.Utils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by kingkz on 11/12/16.
 */
public class MatricesMapper extends Mapper<LongWritable, CellArrayWritable,
        LongWritable, Writable> {

    private long totalRecs;
    private int iter;

    private List<ROWCOLWritable> buffer;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        totalRecs = Long.valueOf(
                Utils.readData(PagerankConfig.NUMBER_OF_LINKS, context
                        .getConfiguration())
        );

        iter = Integer.valueOf(context.getConfiguration().get(PagerankConfig
                .ITER_NUM));

        buffer = new ArrayList<>();

    }

    @Override
    protected void map(LongWritable key, CellArrayWritable value,
                       Context context)
            throws IOException, InterruptedException {

        buffer.add(new ROWCOLWritable(key.get(), value));

        if (buffer.size() >= PagerankConfig.ROWCOL_BLOCK_SIZE) {
            sendBlockData(context);
        }

    }

    private void sendBlockData(Context context) throws IOException, InterruptedException {

        context.write(
                new LongWritable(),
                new ROWCOLArrayWritable(buffer)
        );
        buffer.clear();
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (buffer.size() > 0) {
            sendBlockData(context);
        }
    }
}
