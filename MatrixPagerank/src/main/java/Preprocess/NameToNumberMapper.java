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

    private Map<String, Integer> linknameMap;
    private MultipleOutputs mos;
//    private IntWritable randomkey;
//    private Random random;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        try {
            linknameMap = readLinkMap(context);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        if (linknameMap == null) {
            throw new IOException("can not find linkmap");
        }
        mos = new MultipleOutputs<>(context);
//        randomkey = new IntWritable();
//        random = new Random();
    }

    private Map<String, Integer> readLinkMap(Context context) throws IOException, URISyntaxException {
        URI[] uriArray = context.getCacheFiles();
//        if (uriArray.length == 0) {
//            throw new IOException("Empty Cache Files");
//        }
//
//        String path;
//        if (uriArray != null) {
//            System.out.println(uriArray.length);
//            for (URI uri : uriArray) {
//                System.out.println(uri.toString() + "contains: " + uri
//                        .toString().contains(PagerankConfig.OUTPUT_LINKMAP));
//                if (uri.toString().contains(PagerankConfig.OUTPUT_LINKMAP)) {
//                    path = uri.getPath();

//        String basedir = context.getConfiguration().get(PagerankConfig
//                .OUTPUT_WORKING_DIRECTORY) +
//                "/" +
//        Utils.getPathInTemp(context.getConfiguration(),
//                PagerankConfig
//                        .OUTPUT_LINKMAP + "/-r-00000")
//                .toString();
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

        Map<String, Integer> nameMap = new HashMap<>();
        for (FileStatus file : fs.listStatus(new Path(basedir))) {
            readNameMap(file.getPath(), nameMap, context);
        }
//                }
//            }
//        } else {
//            System.out.println("empty uriArray");
//        }
//        return null;
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
        System.out.println("map size" + nameMap.size());
    }


    @Override
    protected void map(GraphKeyWritable key, TextCellArrayWritable value, Context context)
            throws IOException, InterruptedException {

        List<CellWritable> list = new ArrayList<>();

        for (Writable w : value.get()) {
            TextCellWritable textCell = (TextCellWritable) w;
//            randomkey.set(random.nextInt());
            list.add(
                    new CellWritable(
                            linknameMap.get(textCell.getRowcol()),
//                            random.nextInt(),
                            textCell.getValue()
                    )
            );
        }

        try {
            if (key.getType() == PagerankConfig.OUTLINK_TYPE) {
//                randomkey.set(random.nextInt());
                mos.write(
                        PagerankConfig.OUTPUT_OUTLINKS_MAPPED,
                        new IntWritable(linknameMap.get(key.getName())),
//                        randomkey,
                        new CellArrayWritable(list),
                        PagerankConfig.OUTPUT_OUTLINKS_MAPPED + "/"
                );
            } else {
//                randomkey.set(random.nextInt());
                mos.write(
                        PagerankConfig.OUTPUT_INLINKS_MAPPED,
                        new IntWritable(linknameMap.get(key.getName())),
//                        randomkey,
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
