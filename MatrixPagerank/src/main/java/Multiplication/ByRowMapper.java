package Multiplication;

import Config.PagerankConfig;
import Preprocess.CellArrayWritable;
import Preprocess.CellWritable;
import Util.Utils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by kingkz on 11/12/16.
 */
public class ByRowMapper extends Mapper<IntWritable, CellArrayWritable,
        IntWritable, DoubleWritable> {

    private long numberOfLinks;
    private int iterNumber;
    private boolean isByRow;
    private double counter;
    private Map<Integer, Double> pagerankMap;
    private double lastDanglingSum;
    private Map<Integer, Double> pagerankValueMap;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        if (context.getConfiguration().get(PagerankConfig.URI_ROOT) !=
                null) {
            if (context.getConfiguration().get(PagerankConfig
                    .NUMBER_OF_LINKS) != null)
                numberOfLinks = context.getConfiguration().getLong(PagerankConfig
                        .NUMBER_OF_LINKS, 0);
            else {
                throw new IOException("Can not read number of Links");
            }
        } else {
            numberOfLinks = Long.valueOf(
                    Utils.readData(PagerankConfig.NUMBER_OF_LINKS, context
                            .getConfiguration())
            );
        }

        iterNumber = Integer.valueOf(context.getConfiguration().get(PagerankConfig
                .ITER_NUM));

        isByRow = context.getConfiguration().getBoolean(PagerankConfig
                .PARTITION_BY_ROW, true);

        counter = 0.0;

        lastDanglingSum = context.getConfiguration().getDouble(PagerankConfig
                .DANGLING_NAME, 0);

        pagerankValueMap = new HashMap<>();

        try {
            pagerankMap = readPagerankValue(context);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error reading pagerankvalues");
            throw new IOException("by row mapper setup");
        }
    }


    private Map<Integer, Double> readPagerankValue(Context context) throws
            IOException, URISyntaxException {
//        URI[] uriArray = context.getCacheFiles();
//
//        URI path = null;
//        if (uriArray != null) {
//            for (URI uri : uriArray) {
//                System.out.println("current checking uri: " + uri.toString());
//                System.out.println("pagerankdir: " + PagerankConfig
//                        .OUTPUT_PAGERANK + iterNumber);
//                System.out.println(uri.toString().contains(PagerankConfig
//                        .OUTPUT_PAGERANK + iterNumber));
//                if (uri.toString().contains(PagerankConfig
//                        .OUTPUT_PAGERANK + iterNumber)) {
//                    path = uri;
//                    System.out.println("Im not NULL: !!!" + path);
//                    break;
//                }
//            }
//        }

//        if (path == null) {
//            String pathStr = PagerankConfig
//                    .OUTPUT_PAGERANK +
//                    iterNumber;
//            if (iterNumber == 1) {
//                pathStr += "/-r-00000";
//            } else {
//                pathStr += "/part-r-00000";
//            }
//            path = Utils.getPathInTemp(context.getConfiguration(), pathStr).toUri();
//        }

//
//        System.out.println("final path: " + path);
        String pathStr = context.getConfiguration().get(PagerankConfig
                .OUTPUT_WORKING_DIRECTORY) + "/" + Utils.getPathInTemp(context
                .getConfiguration(), PagerankConfig
                .OUTPUT_PAGERANK+iterNumber);
//        if (iterNumber == 1) {
//            pathStr += "/-r-00000";
//        } else {
//            pathStr += "/part-r-00000";
//        }

        Map<Integer, Double> pagerank = new HashMap<>();

        FileSystem fs = FileSystem.get(new URI(context.getConfiguration().get
                (PagerankConfig.OUTPUT_WORKING_DIRECTORY)), context
                .getConfiguration());

        for (FileStatus file : fs.listStatus(new Path(pathStr))) {
            if (file.getPath().getName().startsWith("-r") ||
                    file.getPath().getName().startsWith("part"))
            readFileToMap(file.getPath(), pagerank, context);
        }

        return pagerank;
    }


    public Map<Integer, Double> readFileToMap(Path path, Map<Integer, Double> pagerank, Context
            context) throws IOException {
        System.out.println("final: " + path.toString());

        SequenceFile.Reader reader = new SequenceFile.Reader(context
                .getConfiguration(), SequenceFile.Reader.file
                (path));
        while (true) {
            IntWritable key = new IntWritable();
            DoubleWritable value = new DoubleWritable();

            if (!reader.next(key, value)) {
                reader.close();
                break;
            }
            double v;
            if (iterNumber == 1) {
                v = 1.0 / numberOfLinks;
            } else {
                v = (lastDanglingSum/numberOfLinks + value.get()) * 0.85 +
                        0.15 / numberOfLinks;
            }

            counter += v;

            pagerank.put(key.get(), v);
        }

        return pagerank;
    }

    private void calculateValueByRow(int rowId, CellArrayWritable cells)
            throws IOException, InterruptedException {

        for (Writable cell : cells.get()) {
            CellWritable c = (CellWritable) cell;

            if (rowId == PagerankConfig.EMPTY_INLINKS_INT) {
                pagerankValueMap.put(c.getRowcol(), 0.0);
            } else {

                if (!pagerankValueMap.containsKey(rowId))
                    pagerankValueMap.put(rowId, 0.0);

                if (pagerankMap.containsKey(c.getRowcol())) {
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
    }


    private void calculateByCol(int rowId, CellArrayWritable cells) {
        if (rowId == PagerankConfig.EMPTY_INLINKS_INT) {
            for (Writable w : cells.get()) {
                CellWritable cell = (CellWritable) w;
                pagerankValueMap.put(cell.getRowcol(), 0.0);
            }
        } else if (rowId == PagerankConfig.DANGLING_NAME_INT) {
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
