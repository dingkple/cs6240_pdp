package Multiplication;

import Config.PagerankConfig;
import Preprocess.CellArrayWritable;
import Preprocess.CellWritable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;

import java.io.IOException;
import java.util.*;

/**
 * Created by kingkz on 11/12/16.
 */
public class PRValueV1Mapper extends Mapper<IntWritable,
        DoubleWritable, IntWritable, ROWCOLArrayWritable> {
    private int blockNum;
    private double lastDanglingSum;
    private Long numberOfLinks;
    private int iterNumber;
    private int counter;
    private List<CellWritable> values;
    private boolean isByRow;
    private Long blockSize;
    Map<Integer, Set<CellWritable>> blockMap;
    private double sumD;
    private double sumO;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //todo read number of records, used for splitting matrix
        blockNum = context.getConfiguration().getInt(
                String.valueOf(PagerankConfig.ROWCOL_BLOCK_SIZE_STRING), -1);
        lastDanglingSum = context.getConfiguration().getDouble(PagerankConfig
                .DANGLING_NAME, 0);

        numberOfLinks = context.getConfiguration().getLong(PagerankConfig
                .NUMBER_OF_LINKS, 0);

        iterNumber = context.getConfiguration().getInt(PagerankConfig
                .ITER_NUM, -1);

        counter = 0;

        values = new ArrayList<>();

        isByRow = context.getConfiguration().getBoolean(PagerankConfig
                .PARTITION_BY_ROW, true);

        blockSize = context.getConfiguration().getLong(String.valueOf(PagerankConfig
                .ROWCOL_BLOCK_SIZE_STRING), 0);

        blockMap = new HashMap<>();

        sumD = 0;
        sumO = 0;
    }

    @Override
    protected void map(IntWritable key, DoubleWritable value, Context context)
            throws IOException, InterruptedException {
        //todo do the group by col thing:
        double v;

        if (iterNumber == 1) {
            v = 1.0 / numberOfLinks;
        } else {
            v = (lastDanglingSum/numberOfLinks + value.get()) * 0.85 + 0.15 /
                    numberOfLinks;
        }

        counter += 1;

        CellWritable newCell = new CellWritable(key.get(), v);
        if (isByRow) {
            values.add(newCell);
        } else {

            int k = (int) (key.get() % blockSize);
            if (k < 0) k += blockSize;
            if (!blockMap.containsKey(k)) {
                blockMap.put(k, new HashSet<>());
            }
            blockMap.get(k).add(newCell);
        }
    }


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("total counter: " + counter + " ");

        if (isByRow) {
            for (int i = 0; i < blockNum; i++) {
                context.write(
                        new IntWritable(i),
                        new ROWCOLArrayWritable(new ROWCOLWritable[]{
                                new ROWCOLWritable(
                                        PagerankConfig.PAGERANK_COL.hashCode(),
                                        new CellArrayWritable(values))})
                );
            }
        } else {
            for (int k : blockMap.keySet()) {
                CellArrayWritable cellArrayWritable = new CellArrayWritable
                        (blockMap.get(k));
                context.write(
                        new IntWritable(k),
                        new ROWCOLArrayWritable(new ROWCOLWritable[]{
                                new ROWCOLWritable(
                                        PagerankConfig.PAGERANK_COL.hashCode(),
                                        cellArrayWritable

                                )
                        })
                );
            }
        }
    }
}
