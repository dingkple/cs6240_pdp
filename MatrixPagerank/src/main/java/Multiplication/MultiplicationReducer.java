package Multiplication;

import Config.PagerankConfig;
import Preprocess.CellArrayWritable;
import Preprocess.CellWritable;
import Util.Utils;
import com.sun.rowset.internal.Row;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * Created by kingkz on 11/12/16.
 */
public class MultiplicationReducer extends Reducer<IntWritable, ROWCOLWritable,
        IntWritable, DoubleWritable> {

    private List<ROWCOLWritable> rowdata;
    private Map<Integer, Double> pagerankValueMap;
    private int prblockSize;
    private Map<Integer, Double> pagerankOldMap;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        rowdata = new ArrayList<>();
        pagerankValueMap = new HashMap<>();
        prblockSize = PagerankConfig.PR_BLOCK_SIZE;
        pagerankOldMap = new HashMap<>();
    }

    @Override
    protected void reduce(IntWritable key, Iterable<ROWCOLWritable> values,
                          Context context) throws IOException,
            InterruptedException {

        Iterator<ROWCOLWritable> iter = values.iterator();
        while (iter.hasNext()) {
            ROWCOLWritable w = iter.next();
//            if (pagerankOldMap.size() < prblockSize) {
//                pagerankOldMap.put(w.getId(), w.)
//            }
            if (w.getId() != PagerankConfig.PAGERANK_COL.hashCode()) {
                this.rowdata.add(new ROWCOLWritable(w));
                pagerankValueMap.put(w.getId(), 0.0);
            }
        }

    }

    private void calculateValue(CellWritable pagerank) {
        for (ROWCOLWritable rowcol : rowdata) {
            for (Writable cell : rowcol.getCellArray().get()) {
                CellWritable c = (CellWritable) cell;
                if (c.getRowcol() == pagerank.getRowcol()) {
                    pagerankValueMap.put(
                            c.getRowcol(),
                            pagerankValueMap.get(c.getRowcol()) + c.getValue
                                    () * pagerank.getValue());
                }
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Integer linkHashcode : pagerankValueMap.keySet()) {
            if (linkHashcode == PagerankConfig.DANGLING_NAME.hashCode()) {
                Utils.writeData(
                        PagerankConfig.DANGLING_FILENAME,
                        String.valueOf(pagerankValueMap.get(linkHashcode)),
                        context.getConfiguration());
            } else {
                context.write(
                        new IntWritable(linkHashcode),
                        new DoubleWritable(pagerankValueMap.get(linkHashcode))
                );
            }
        }
    }
}
