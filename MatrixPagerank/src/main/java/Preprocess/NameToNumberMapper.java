package Preprocess;

import Config.PagerankConfig;
import Util.Utils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

/**
 * Created by kingkz on 11/15/16.
 */
public class NameToNumberMapper extends Mapper<GraphKeyWritable, TextCellArrayWritable,
        IntWritable, CellArrayWritable> {

    private Map<String, Integer> linkNameMap;
    private MultipleOutputs mos;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        try {
            linkNameMap = readLinkMap(context);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        if (linkNameMap == null) {
            throw new IOException("can not find linkmap");
        }
        mos = new MultipleOutputs<>(context);

    }

    private Map<String, Integer> readLinkMap(Context context) throws IOException, URISyntaxException {

        String basedir = context.getConfiguration().get(PagerankConfig
                .OUTPUT_WORKING_DIRECTORY) +
                "/" +
                Utils.getPathInTemp(context.getConfiguration(),
                        PagerankConfig
                                .OUTPUT_LINKMAP)
                        .toString();

        FileSystem fs = FileSystem.get(new URI(context.getConfiguration().get
                (PagerankConfig.OUTPUT_WORKING_DIRECTORY)), context
                .getConfiguration());

        Map<String, Integer> nameMap = new HashMap<>();
        for (FileStatus file : fs.listStatus(new Path(basedir))) {
            readNameMap(file.getPath(), nameMap, context);
        }

        nameMap.put(PagerankConfig.DANGLING_NAME, PagerankConfig
                .DANGLING_NAME_INT);
        nameMap.put(PagerankConfig.EMPTY_INLINKS, PagerankConfig
                .EMPTY_INLINKS_INT);
        return nameMap;
    }

    private void readNameMap(Path path, Map<String, Integer> nameMap, Context context) throws IOException {
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
            nameMap.put(key.toString(), value.get());
        }
    }


    @Override
    protected void map(GraphKeyWritable key, TextCellArrayWritable value, Context context)
            throws IOException, InterruptedException {

        List<CellWritable> list = new ArrayList<>();

        for (Writable w : value.get()) {
            TextCellWritable textCell = (TextCellWritable) w;
            list.add(
                    new CellWritable(
                            linkNameMap.get(textCell.getRowcol()),
                            textCell.getValue()
                    )
            );
        }

        try {
            if (key.getType() == PagerankConfig.OUTLINK_TYPE) {
                mos.write(
                        PagerankConfig.OUTPUT_OUTLINKS_MAPPED,
                        new IntWritable(linkNameMap.get(key.getName())),
                        new CellArrayWritable(list),
                        PagerankConfig.OUTPUT_OUTLINKS_MAPPED + "/"
                );
            } else {
                mos.write(
                        PagerankConfig.OUTPUT_INLINKS_MAPPED,
                        new IntWritable(linkNameMap.get(key.getName())),
                        new CellArrayWritable(list),
                        PagerankConfig.OUTPUT_INLINKS_MAPPED + "/"
                );
            }
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
