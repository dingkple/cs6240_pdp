package Multiplication;

import Config.PagerankConfig;
import Preprocess.CellArrayWritable;
import Util.Utils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by kingkz on 11/12/16.
 */
public class MatricesMapper extends Mapper<IntWritable, Writable,
        IntWritable, ROWCOLArrayWritable> {

    private int blockNum;
    private Map<Integer, List<ROWCOLWritable>> blockMap;
    private boolean isByRow;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        blockNum = context.getConfiguration().getInt(String.valueOf(PagerankConfig
                .ROWCOL_BLOCK_SIZE_STRING), -1);
        
        blockMap = new HashMap<>();

        isByRow = context.getConfiguration().getBoolean(PagerankConfig
                .PARTITION_BY_ROW, true);
    }

    @Override
    protected void map(IntWritable key, Writable value,
                       Context context) {

        int block_id = key.get() % blockNum;
        if (block_id < 0) block_id += blockNum;
        if (!blockMap.containsKey(block_id)) {
            blockMap.put(block_id, new ArrayList<>());
        }
    if (key.get() == PagerankConfig.DANGLING_NAME_INT) {
            ROWCOLWritable danglingRow = new ROWCOLWritable(
                    key.get(),
                    (CellArrayWritable) value
            );

            if (!isByRow) {
                for (int i = 0; i < blockNum; i++) {
                    if (!blockMap.containsKey(i)) {
                        blockMap.put(i, new ArrayList<>());
                    }
                    blockMap.get(i).add(danglingRow);
                }
            } else {
                blockMap.get(block_id).add(danglingRow);
            }
        } else {
            blockMap.get(block_id).add(
                    new ROWCOLWritable(
                            key.get(),
                            (CellArrayWritable) value
                    )
            );
        }
    }


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Integer block : blockMap.keySet()) {
            context.write(
                    new IntWritable(block),
                    new ROWCOLArrayWritable(blockMap.get(block))
            );
        }
    }
}
