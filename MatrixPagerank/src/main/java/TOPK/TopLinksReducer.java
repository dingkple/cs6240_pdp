package TOPK;

import Config.PagerankConfig;
import Util.Utils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

/**
 * Created by kingkz on 11/13/16.
 */

public class TopLinksReducer
        extends Reducer<DoubleWritable, IntWritable, DoubleWritable,
        Text> {

    private static TreeMap<Double, String> topMap;
    private Map<Integer, String> linkMap;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        topMap = new TreeMap<>();
        try {
            linkMap = readLinkMap(context);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    private Map<Integer, String> readLinkMap(Context context) throws
            IOException, URISyntaxException {

        String basedir = context.getConfiguration().get(PagerankConfig
                .OUTPUT_WORKING_DIRECTORY) +
                "/" +
                Utils.getPathInTemp(context.getConfiguration(),
                        PagerankConfig
                                .OUTPUT_LINKMAP)
                        .toString();
        System.out.println("found cache");
        FileSystem fs = FileSystem.get(new URI(context.getConfiguration().get
                (PagerankConfig.OUTPUT_WORKING_DIRECTORY)), context
                .getConfiguration());

        Map<Integer, String> nameMap = new HashMap<>();
        for (FileStatus file : fs.listStatus(new Path(basedir))) {
            readNameMap(file.getPath(), nameMap, context);
        }

        nameMap.put(PagerankConfig
                .DANGLING_NAME_INT, PagerankConfig.DANGLING_NAME);
        nameMap.put(PagerankConfig.EMPTY_INLINKS_INT, PagerankConfig.EMPTY_INLINKS);
        return nameMap;
    }

    private void readNameMap(Path path, Map<Integer, String> nameMap, Context
            context) throws IOException {
        SequenceFile.Reader reader = new SequenceFile.Reader(context
                .getConfiguration(), SequenceFile.Reader.file
                (path));

        Text key = new Text();
        IntWritable value = new IntWritable();
        while (true) {
            if (!reader.next(key, value)) {
                reader.close();
                break;
            }
            nameMap.put(value.get(), key.toString());
        }
        System.out.println("map size" + nameMap.size());
    }

    /**
     * Write the first 100
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(DoubleWritable key, Iterable<IntWritable> values,
                          Context context)
            throws IOException, InterruptedException {
//        double pagerank = 0;
//        String linkname = null;
//        int counter = 0;
//        for (PagerankCellWritable cell : values) {
//            if (cell.getName().length() > 0) {
//                linkname = cell.getName();
//                counter += 1;
//            } else if (cell.getPagerank() > 0) {
//                pagerank = cell.getPagerank();
//                counter += 1;
//            }
//
//        }
//        if (counter == 2) {
        for (IntWritable link : values) {
            topMap.put(key.get(), linkMap.get(link.get()));
            if (topMap.size() > 100) {
                topMap.remove(topMap.firstKey());
            }
        }
//        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        List<Double> values = new ArrayList<>();
        values.addAll(topMap.keySet());
        values.sort((o1, o2) -> o2.compareTo(o1));
        for (double weight : values) {
            context.write(new DoubleWritable(weight), new Text(topMap.get
                    (weight)));
        }
    }
}