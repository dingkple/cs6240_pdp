package Preprocess;

import Config.PagerankConfig;
import Util.Utils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by kingkz on 11/15/16.
 */
public class NameToNumberMapper extends Mapper<GraphKeyWritable, TextCellArrayWritable,
        IntWritable, CellArrayWritable> {

    private Map<String, Integer> linknameMap;
    private MultipleOutputs mos;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        linknameMap = readLinkMap(context);
        mos = new MultipleOutputs<>(context);
    }

    private Map<String, Integer> readLinkMap(Context context) throws IOException {
        URI[] uriArray = context.getCacheFiles();

        String path = null;
        if (uriArray != null) {
            for (URI uri : uriArray) {
                if (uri.getPath().contains(PagerankConfig.OUTPUT_LINKMAP)) {
                    path = uri.getPath();
                    break;
                }
            }
        }

        if (path == null) {
            path = Utils.getPathInTemp(PagerankConfig.OUTPUT_LINKMAP).toString();
        }

        SequenceFile.Reader reader = new SequenceFile.Reader(context
                .getConfiguration(), SequenceFile.Reader.file
                (new Path(path+"/-r-00000")));

        Map<String, Integer> map = new HashMap<>();
        while (true) {
            Text key = new Text();
            IntWritable value = new IntWritable();

            if (!reader.next(key, value)) {
                break;
            }
            map.put(key.toString(), value.get());
        }

        return map;
    }

    @Override
    protected void map(GraphKeyWritable key, TextCellArrayWritable value, Context context)
            throws IOException, InterruptedException {

        List<CellWritable> list = new ArrayList<>();

        for (Writable w : value.get()) {
            TextCellWritable textCell = (TextCellWritable) w;
            list.add(
                    new CellWritable(
                            linknameMap.get(textCell.getRowcol()),
                            textCell.getValue()
                    )
            );
        }

        try {
            if (key.getType() == PagerankConfig.OUTLINK_TYPE) {
                mos.write(
                        PagerankConfig.OUTPUT_OUTLINKS_MAPPED,
                        new IntWritable(linknameMap.get(key.getName())),
                        new CellArrayWritable(list),
                        PagerankConfig.OUTPUT_OUTLINKS_MAPPED + "/"
                );
            } else {
                mos.write(
                        PagerankConfig.OUTPUT_INLINKS_MAPPED,
                        new IntWritable(linknameMap.get(key.getName())),
                        new CellArrayWritable(list),
                        PagerankConfig.OUTPUT_INLINKS_MAPPED + "/"
                );
            }
//            key.setCount(linknameMap.get(key.getName()));
//            context.write(
//                    key,
//                    new CellArrayWritable(list)
//            );
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("No name found");
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }
}
