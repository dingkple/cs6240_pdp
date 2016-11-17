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

    private long numberOfLinks;
    private int iterNumber;
    private int blockNum;
    private Map<Integer, List<ROWCOLWritable>> blockMap;
    private boolean isByRow;
    private double counter;
    private Map<Integer, Double> pagerankMap;
    private double lastDanglingSum;
    private Map<Integer, Double> pagerankValueMap;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        numberOfLinks = Long.valueOf(
                Utils.readData(PagerankConfig.NUMBER_OF_LINKS, context
                        .getConfiguration())
        );

        iterNumber = Integer.valueOf(context.getConfiguration().get(PagerankConfig
                .ITER_NUM));

        blockNum = context.getConfiguration().getInt(String.valueOf(PagerankConfig
                .ROWCOL_BLOCK_SIZE_STRING), -1);
        
        blockMap = new HashMap<>();

        isByRow = context.getConfiguration().getBoolean(PagerankConfig
                .PARTITION_BY_ROW, true);

        counter = 0.0;

        pagerankMap = readPagerankValue(context);

        lastDanglingSum = context.getConfiguration().getDouble(PagerankConfig
                .DANGLING_NAME, 0);

        pagerankValueMap = new HashMap<>();
    }


    private Map<Integer, Double> readPagerankValue(Context context) throws
            IOException {
        URI[] uriArray = context.getCacheFiles();

        String path = null;
        if (uriArray != null) {
            for (URI uri : uriArray) {
                if (uri.getPath().contains(PagerankConfig
                        .OUTPUT_PAGERANK+ iterNumber)) {
                    path = uri.getPath();
                    break;
                }
            }
        }

        if (path == null) {
            path = Utils.getPathInTemp(context.getConfiguration(), PagerankConfig
                    .OUTPUT_PAGERANK +
                    iterNumber)
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
            double v;
            if (iterNumber == 1) {
                v = 1.0 / numberOfLinks;
            } else {
                v = (lastDanglingSum/numberOfLinks + value.get()) * 0.85 + 0.15 /
                        numberOfLinks;
            }

            map.put(key.get(), v);
        }

        return map;
    }

    private void calculateValueByRow(ROWCOLWritable rowcol)
            throws IOException, InterruptedException {
            boolean isEmpltyInlink = rowcol.getId() == PagerankConfig
                    .EMPTY_INLINKS.hashCode();
            for (Writable cell : rowcol.getCellArray().get()) {
                CellWritable c = (CellWritable) cell;

                if (isEmpltyInlink) {
                    pagerankValueMap.put(c.getRowcol(), 0.0);
                    continue;
                }
                if (!pagerankValueMap.containsKey(rowcol.getId()))
                    pagerankValueMap.put(rowcol.getId(), 0.0);

                if (pagerankMap.containsKey(c.getRowcol())) {
                    double change = c.getValue()
                            * pagerankMap.get(c.getRowcol());
                    pagerankValueMap.put(
                            rowcol.getId(),
                            pagerankValueMap.get(rowcol.getId())
                                    + change);
                } else {
                    System.out.println("fuck");
                }
        }
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
