package Multiplication;

import Config.PagerankConfig;
import Preprocess.CellArrayWritable;
import Preprocess.CellWritable;
import Util.Utils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by kingkz on 11/12/16.
 */
public class MatricesMapper extends Mapper<IntWritable, Writable,
        IntWritable, ROWCOLArrayWritable> {

    private long totalRecs;
    private int iter;
    private int blockNum;
    private Map<Integer, List<ROWCOLWritable>> blockMap;
    private boolean isByRow;
    private double counter;
    private Map<Integer, Double> paegrankMap;

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
        
        blockMap = new HashMap<>();

        isByRow = context.getConfiguration().getBoolean(PagerankConfig
                .PARTITION_BY_ROW, true);

        counter = 0.0;

        paegrankMap = readPagerankValue(context);
    }


    private Map<Integer, Double> readPagerankValue(Context context) throws
            IOException {
        URI[] uriArray = context.getCacheFiles();

        String path = null;
        if (uriArray != null) {
            for (URI uri : uriArray) {
                if (uri.getPath().contains(PagerankConfig
                        .OUTPUT_PAGERANK+iter)) {
                    path = uri.getPath();
                    break;
                }
            }
        }

        if (path == null) {
            path = Utils.getPathInTemp(PagerankConfig.OUTPUT_PAGERANK+iter)
                    .toString();
        }

        SequenceFile.Reader reader = new SequenceFile.Reader(context
                .getConfiguration(), SequenceFile.Reader.file
                (new Path(path+"/-r-00000")));

        Map<Integer, Double> map = new HashMap<>();
        while (true) {
            IntWritable key = new IntWritable();
            DoubleWritable value = new DoubleWritable();

            if (!reader.next(key, value)) {
                break;
            }
            map.put(key.get(), value.get());
        }

        return map;
    }

    @Override
    protected void map(IntWritable key, Writable value,
                       Context context) {
        int keyInt = key.get();
//        int block_id = key.get() % blockNum;
        int block_id;

        if (keyInt < 0) block_id = 0;
        else {
            block_id = key.get() % (blockNum - 1) + 1;
        }


        if (!blockMap.containsKey(block_id)) {
            blockMap.put(block_id, new ArrayList<>());
        }
        CellArrayWritable cells = (CellArrayWritable) value;
        for (Writable w1 : cells.get()) {
            CellWritable c = (CellWritable) w1;
            counter += c.getValue();
        }
        if (key.get() == PagerankConfig.DANGLING_NAME_INT) {
            ROWCOLWritable danglingRow = new ROWCOLWritable(
                    key.get(),
                    cells
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
                            cells
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

        System.out.println(counter);
    }
}
