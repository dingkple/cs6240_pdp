package Preprocess;

import Config.PagerankConfig;
import Util.Utils;
import com.google.common.collect.Iterables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by kingkz on 11/11/16.
 */
public class GraphCreaterMapper extends Mapper<GraphKeyWritable, TextArrayWritable,
        GraphKeyWritable, CellArrayWritable> {
    private Map<String, Long> linknameMap;


    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        linknameMap = new HashMap<>();
        readLinks(context.getConfiguration());

    }

    private void readLinks(Configuration conf) throws IOException {

            Path path = new Path(Utils.getPathInTemp(PagerankConfig
                    .OUTPUT_LINKMAP)
                    .toString() + "/-r-00000");
            SequenceFile.Reader rdr = new SequenceFile.Reader(conf,
                    SequenceFile.Reader.file(path));

            while (true) {
//                GraphKeyWritable _k = new GraphKeyWritable();
//                TextArrayWritable links = new TextArrayWritable();

                try {
                    Text _k = new Text();
                    LongWritable links = new LongWritable();
                    if (!rdr.next(_k, links)) {
                        break;
                    }

//                this.linknameMap.put(_k.getName(), Long.valueOf(links.get()[0]
//                        .toString()));
                    this.linknameMap.put(_k.toString(), links.get());
                } catch (Exception e) {
//                    System.out.println
                }

            }
            rdr.close();
//        }
    }

    @Override
    protected void map(GraphKeyWritable key, TextArrayWritable value, Context
            context)
            throws IOException, InterruptedException {
        ArrayList<CellWritable> cells = new ArrayList<>();
        for (Writable name : value.get()) {
            cells.add(new CellWritable(this.linknameMap.get(name.toString
                    ()), 1.0/value.get().length));
        }
        Long type = key.getType();
        String name = key.getName();

        if (type.longValue() == PagerankConfig.OUTLINK_TYPE) {
            context.write(
                    new GraphKeyWritable(
                            PagerankConfig.KEY_TYEP_ROW,
                            this.linknameMap.get(name)),
                    new CellArrayWritable(Iterables.toArray(cells, CellWritable
                            .class)));
        } else {
            // TODO: 11/11/16 Writa inlink list
            context.write(new GraphKeyWritable(PagerankConfig.KEY_TYPE_COL, this
                    .linknameMap.get(name)),
                    new CellArrayWritable(Iterables.toArray(cells, CellWritable
                            .class)));
        }
    }

}
