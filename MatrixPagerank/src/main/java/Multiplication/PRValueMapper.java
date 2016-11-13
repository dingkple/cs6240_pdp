package Multiplication;

import Config.PagerankConfig;
import Preprocess.CellArrayWritable;
import Preprocess.CellWritable;
import Util.Utils;
import com.google.common.collect.Iterables;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by kingkz on 11/12/16.
 */
public class PRValueMapper extends Mapper<LongWritable,
        DoubleWritable, LongWritable, Writable> {
    private List<CellWritable> buffer;
    private Long totalRecs;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //todo read number of records, used for splitting matrix
//        buffer = new CellWritable[(int) (PagerankConfig.PR_BLOCK_SIZE+1)];
        buffer = new ArrayList<>();
        totalRecs = Long.valueOf(Utils.readData(PagerankConfig
                .NUMBER_OF_LINKS, context.getConfiguration()));
    }

    @Override
    protected void map(LongWritable key, DoubleWritable value, Context context)
            throws IOException, InterruptedException {

        //todo do the group by col thing:
//        buffer[bufferCounter++] = new CellWritable(key.get(), value.get());
        buffer.add(new CellWritable(key.get(), value.get()));
        if (buffer.size()> PagerankConfig.PR_BLOCK_SIZE) {
            sendPRBlock(context);
        }

    }

    private void sendPRBlock(Context context)
            throws IOException, InterruptedException {

        for (int i = 0; i <= totalRecs/PagerankConfig.ROWCOL_BLOCK_SIZE; i++) {
            context.write(
                    new LongWritable(i),
                    new CellArrayWritable(Iterables.toArray(buffer,
                            CellWritable.class))
            );
        }
        buffer.clear();

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        sendPRBlock(context);
    }
}
