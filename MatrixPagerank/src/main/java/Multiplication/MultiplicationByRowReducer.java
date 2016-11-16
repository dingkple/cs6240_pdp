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
public class MultiplicationByRowReducer extends Reducer<IntWritable, ROWCOLArrayWritable,
        IntWritable, DoubleWritable> {

    private List<ROWCOLWritable> rowdata;
    private Map<Integer, Double> pagerankValueMap;
    private Map<Integer, Double> pagerankOldMap;

    private Map<Integer, Double> tracker;

    private boolean isByRow;
    private double counter1;
    private double counter2;
    private double counter3;
    private int counter4;
    private double counter5;

    private double counter6 = 0;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        rowdata = new ArrayList<>();
        pagerankValueMap = new HashMap<>();
        pagerankOldMap = new HashMap<>();
        isByRow = context.getConfiguration().getBoolean(PagerankConfig
                .PARTITION_BY_ROW, true);
        counter1 = 0.0;
        counter2 = 0.0;
        counter3 = 0.0;
        counter4 = 0;
        counter5 = 0.0;

        tracker = new HashMap<>();
    }

    @Override
    protected void reduce(IntWritable key, Iterable<ROWCOLArrayWritable> values,
                          Context context) throws IOException,
            InterruptedException {

        Iterator<ROWCOLArrayWritable> iter = values.iterator();
        rowdata.clear();
        while (iter.hasNext()) {
            ROWCOLArrayWritable w0 = iter.next();
            for (Writable w1 : w0.get()) {
                ROWCOLWritable w = (ROWCOLWritable) w1;
                int l1 = w.getCellArray().get().length;
                if (w.getId() == PagerankConfig.PAGERANK_COL.hashCode()) {
                    for (Writable w2 : w.getCellArray().get()) {
                        CellWritable cell = (CellWritable) w2;
                        if (!pagerankOldMap.containsKey(cell.getRowcol())) {
                            pagerankOldMap.put(cell.getRowcol(), 0.0);
                        }
                        pagerankOldMap.put(cell.getRowcol(), pagerankOldMap
                                .get(cell.getRowcol()) + cell.getValue());
                        counter3 += cell.getValue();
//                        pagerankOldMap.put(cell.getRowcol(), cell.getValue());
                        if (!tracker.containsKey(cell.getRowcol()))
                            tracker.put(cell.getRowcol(), cell.getValue());
                    }
                    int l2 = pagerankOldMap.size();
                } else {
                    this.rowdata.add(new ROWCOLWritable(w));
                }
            }
        }

        System.out.println(counter3);

        if (isByRow) {
            calculateValueByRow();
            writeValues(context);
        } else {
            calculateValueByCol();
        }
        pagerankOldMap.clear();
    }

    private void writeValues(Context context) throws IOException, InterruptedException {
        for (Integer linkHashcode : pagerankValueMap.keySet()) {
            if (isByRow && linkHashcode == PagerankConfig.DANGLING_NAME.hashCode
                    ()) {
                double dangling = pagerankValueMap.get(linkHashcode);
                System.out.println("dangling: " + dangling);
                Utils.writeData(
                        PagerankConfig.DANGLING_FILENAME,
                        String.valueOf(dangling),
                        context.getConfiguration());
            } else {
                counter4 += 1;
                context.write(
                        new IntWritable(linkHashcode),
                        new DoubleWritable(pagerankValueMap.get(linkHashcode))
                );
                counter2 += pagerankValueMap.get(linkHashcode);
            }
            counter1 += pagerankValueMap.get(linkHashcode);
        }
        pagerankValueMap.clear();
    }

    private void calculateValueByCol() {
        for (ROWCOLWritable rowcol : rowdata) {
            boolean isDangling = rowcol.getId() == PagerankConfig
                    .DANGLING_NAME.hashCode();

            boolean isEmptyLinks = rowcol.getId() == PagerankConfig
                    .EMPTY_INLINKS.hashCode();

            if (isDangling) {
                if (!pagerankValueMap.containsKey(rowcol.getId())) {
                    pagerankValueMap.put(rowcol.getId(), 0.0);
                }
            }

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
//                    System.err.println("rowcol: " + rowcol.getId() + " cell: " +
//                            "" + cell.getRowcol());
//                    System.err.println(rowcol.getId() % 3 + " " + cell
//                            .getRowcol()
//                            % 3);
                }
            }
        }
    }


    private void calculateValueByRow()
            throws IOException, InterruptedException {
        for (ROWCOLWritable rowcol : rowdata) {
            boolean isEmpltyInlink = rowcol.getId() == PagerankConfig
                    .EMPTY_INLINKS.hashCode();
            for (Writable cell : rowcol.getCellArray().get()) {
                CellWritable c = (CellWritable) cell;

                if (isEmpltyInlink) {
                    pagerankValueMap.put(c.getRowcol(), 0.0);
                    tracker.put(c.getRowcol(), 0.0);
                    continue;
                }
                if (!pagerankValueMap.containsKey(rowcol.getId()))
                    pagerankValueMap.put(rowcol.getId(), 0.0);

                if (pagerankOldMap.containsKey(c.getRowcol())) {
                    double change = c.getValue()
                            * pagerankOldMap.get(c.getRowcol());
                    pagerankValueMap.put(
                            rowcol.getId(),
                            pagerankValueMap.get(rowcol.getId())
                                    + change);
                    tracker.put(c.getRowcol(), tracker.get(c.getRowcol()) -
                            change);
                } else {
                    System.out.println("fuck");
                }
//                if (c.getValue() < 1)
                counter6 += c.getValue();
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (!isByRow) {
            writeValues(context);
        }

        double k = 0;
        for (int key: tracker.keySet()) {
            k += tracker.get(key);
        }

        System.out.println(counter1 + " " + counter2 + " " + counter4 + " " + k);
    }
}
