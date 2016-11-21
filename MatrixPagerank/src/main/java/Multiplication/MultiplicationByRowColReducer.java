package Multiplication;

import Config.PagerankConfig;
import Preprocess.CellWritable;
import Util.Utils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * Created by kingkz on 11/12/16.
 */
public class MultiplicationByRowColReducer extends Reducer<IntWritable, ROWCOLArrayWritable,
        IntWritable, DoubleWritable> {

    private List<ROWCOLWritable> rowcoldata;
    private Map<Integer, Double> pagerankValueMap;
    private Map<Integer, Double> pagerankOldMap;

    private boolean isByRow;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        rowcoldata = new ArrayList<>();
        pagerankValueMap = new HashMap<>();
        pagerankOldMap = new HashMap<>();
        isByRow = context.getConfiguration().getBoolean(PagerankConfig
                .PARTITION_BY_ROW, true);
    }

    @Override
    protected void reduce(IntWritable key, Iterable<ROWCOLArrayWritable> values,
                          Context context) throws IOException,
            InterruptedException {

        Iterator<ROWCOLArrayWritable> iter = values.iterator();
        rowcoldata.clear();

        /*
           Get all the row or columns and pagerank vectors from the coming
           values.
         */
        while (iter.hasNext()) {
            ROWCOLArrayWritable w0 = iter.next();
            for (Writable w1 : w0.get()) {
                ROWCOLWritable w = (ROWCOLWritable) w1;
                if (w.getId() == PagerankConfig.PAGERANK_COL_INT) {
                    for (Writable w2 : w.getCellArray().get()) {
                        CellWritable cell = (CellWritable) w2;
                        pagerankOldMap.put(cell.getRowcol(), cell.getValue());
                    }
                } else {
                    this.rowcoldata.add(new ROWCOLWritable(w));
                }
            }
        }

        if (isByRow) {
            calculateValueByRow();
            writeValues(context);
        } else {
            calculateValueByCol();
        }
        pagerankOldMap.clear();
    }

    private void writeValues(Context context) throws IOException, InterruptedException {
        for (int linkNumber : pagerankValueMap.keySet()) {
            if (isByRow && linkNumber == PagerankConfig.DANGLING_NAME_INT) {
                double dangling = pagerankValueMap.get(linkNumber);
                Utils.writeData(
                        PagerankConfig.DANGLING_FILENAME,
                        String.valueOf(dangling),
                        context.getConfiguration());
            } else {
                context.write(
                        new IntWritable(linkNumber),
                        new DoubleWritable(pagerankValueMap.get(linkNumber))
                );
            }
        }
        pagerankValueMap.clear();
    }

    /**
     * Calculate each block
     */
    private void calculateValueByCol() {
        for (ROWCOLWritable rowcol : rowcoldata) {
            boolean isDangling = rowcol.getId() == PagerankConfig
                    .DANGLING_NAME_INT;

            boolean isEmptyLinks = rowcol.getId() == PagerankConfig
                    .EMPTY_INLINKS_INT;

            if (isDangling) {
                if (!pagerankValueMap.containsKey(rowcol.getId())) {
                    pagerankValueMap.put(rowcol.getId(), 0.0);
                }
            }

            // treat 3 kinds of data differently.
            // empty_inlink list is used to ensure pagerank vector containing
            // all the links
            // dangling list is used for compute the sum of dangling links
            for (Writable w : rowcol.getCellArray().get()) {
                CellWritable cell = (CellWritable) w;
                try {
                    if (isEmptyLinks) {
                        if (!pagerankValueMap.containsKey(cell.getRowcol())) {
                            pagerankValueMap.put(cell.getRowcol(), 0.0);
                        }
                    } else {
                        if (!isDangling) {
                            if (pagerankOldMap.containsKey(rowcol.getId())) {
                                if (!pagerankValueMap.containsKey(cell.getRowcol())) {
                                    pagerankValueMap.put(cell.getRowcol(), 0.0);
                                }
                                double v = pagerankValueMap.get(cell.getRowcol());

                                v += pagerankOldMap.get(rowcol.getId()) * cell.getValue();
                                pagerankValueMap.put(cell.getRowcol(), v);
                            }
                        } else {
                            double v = pagerankValueMap.get(rowcol.getId());
                            if (pagerankOldMap.containsKey(cell.getRowcol())) {
                                v += pagerankOldMap.get(cell.getRowcol());
                                pagerankValueMap.put(rowcol.getId(), v);
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }


    private void calculateValueByRow()
            throws IOException, InterruptedException {
        for (ROWCOLWritable rowcol : rowcoldata) {
            boolean isEmpltyInlink = rowcol.getId() == PagerankConfig.EMPTY_INLINKS_INT;
            for (Writable cell : rowcol.getCellArray().get()) {
                CellWritable c = (CellWritable) cell;

                if (isEmpltyInlink) {
                    pagerankValueMap.put(c.getRowcol(), 0.0);
                } else {

                    // dangling links are handled here with other rows, in
                    // pre-processing part, all the dangling links are
                    // compressed as one row now.
                    if (!pagerankValueMap.containsKey(rowcol.getId()))
                        pagerankValueMap.put(rowcol.getId(), 0.0);

                    if (pagerankOldMap.containsKey(c.getRowcol())) {
                        double change = c.getValue()
                                * pagerankOldMap.get(c.getRowcol());
                        pagerankValueMap.put(
                                rowcol.getId(),
                                pagerankValueMap.get(rowcol.getId())
                                        + change);
                    }
                }
            }
            if (rowcol.getId() == PagerankConfig.DANGLING_NAME_INT) {
                System.out.println(pagerankValueMap.get(rowcol.getId()));
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (!isByRow) {
            writeValues(context);
        }
    }
}
