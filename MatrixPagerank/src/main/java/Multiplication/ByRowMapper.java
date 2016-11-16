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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by kingkz on 11/12/16.
 */
public class ByRowMapper extends Mapper<IntWritable, CellArrayWritable,
        IntWritable, DoubleWritable> {

    private long numberOfLinks;
    private int iterNumber;
    private int blockNum;
    private Map<Integer, List<ROWCOLWritable>> blockMap;
    private boolean isByRow;
    private double counter;
    private Map<Integer, Double> pagerankMap;
    private double lastDanglingSum;
    private Map<Integer, Double> pagerankValueMap;

    private double counter2 = 0;

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


        lastDanglingSum = context.getConfiguration().getDouble(PagerankConfig
                .DANGLING_NAME, 0);

        pagerankValueMap = new HashMap<>();
        pagerankMap = readPagerankValue(context);
    }


    private Map<Integer, Double> readPagerankValue(Context context) throws
            IOException {
        URI[] uriArray = context.getCacheFiles();

        String path = null;
        if (uriArray != null) {
            for (URI uri : uriArray) {
                if (uri.getPath().contains(PagerankConfig
                        .OUTPUT_PAGERANK + iterNumber)) {
                    path = uri.getPath();
                    break;
                }
            }
        }

        if (path == null) {
            path = Utils.getPathInTemp(PagerankConfig.OUTPUT_PAGERANK +
                    iterNumber)
                    .toString();
        }

        if (iterNumber == 1) {
            path += "/-r-00000";
        } else {
            path += "/part-r-00000";
        }

        SequenceFile.Reader reader = new SequenceFile.Reader(context
                .getConfiguration(), SequenceFile.Reader.file
                (new Path(path)));

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

            counter += v;

            map.put(key.get(), v);
        }

        return map;
    }

    private void calculateValueByRow(int rowId, CellArrayWritable cells)
            throws IOException, InterruptedException {

        for (Writable cell : cells.get()) {
            CellWritable c = (CellWritable) cell;

            if (!pagerankValueMap.containsKey(rowId))
                pagerankValueMap.put(rowId, 0.0);

            if (pagerankMap.containsKey(c.getRowcol())) {
                counter2 += c.getValue();
                double change = c.getValue() * pagerankMap.get(c.getRowcol());
                pagerankValueMap.put(
                        rowId,
                        pagerankValueMap.get(rowId)
                                + change);
            } else {
                System.out.println("fuck");
            }
        }
    }


    private void calculateByCol(int rowId, CellArrayWritable cells) {
        if (rowId == PagerankConfig.DANGLING_NAME_INT) {
            double v = 0;
            if (!pagerankValueMap.containsKey(rowId))
                pagerankValueMap.put(rowId, 0.0);
            for (Writable w : cells.get()) {
                CellWritable cell  = (CellWritable) w;
                v += pagerankMap.get(cell.getRowcol());
            }
            pagerankValueMap.put(rowId, v);
        } else {
            for (Writable cell : cells.get()) {
                CellWritable c = (CellWritable) cell;
                if (!pagerankValueMap.containsKey(c.getRowcol())) {
                    pagerankValueMap.put(c.getRowcol(), 0.0);
                }
                if (pagerankMap.containsKey(rowId)) {
                    pagerankValueMap.put(
                            c.getRowcol(),
                            pagerankValueMap.get(
                                    c.getRowcol()) + c.getValue() *
                                    pagerankMap.get(rowId)
                    );
                }
            }
        }
    }

    @Override
    protected void map(IntWritable key, CellArrayWritable value,
                       Context context) throws IOException, InterruptedException {
        if (isByRow)
            calculateValueByRow(key.get(), value);
        else
            calculateByCol(key.get(), value);
    }


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (int i : pagerankValueMap.keySet()) {
            context.write(
                    new IntWritable(i),
                    new DoubleWritable(pagerankValueMap.get(i))
            );
        }

        System.out.println(counter);
    }
}
