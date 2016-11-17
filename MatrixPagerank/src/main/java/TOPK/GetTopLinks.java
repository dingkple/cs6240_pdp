package TOPK;

import Config.PagerankConfig;
import Util.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeMap;

/**
 * Created by kingkz on 11/13/16.
 */
public class GetTopLinks {


    public static void showTop(Configuration conf, boolean isByRow) throws
            Exception {

        Job job = Job.getInstance(conf);

        job.setJarByClass(GetTopLinks.class);

        Path pagerankInput;

        pagerankInput = Utils.getPathInTemp(conf, PagerankConfig
                .OUTPUT_PAGERANK + "11");

        MultipleInputs.addInputPath(
                job,
                pagerankInput,
                SequenceFileInputFormat.class,
                TopLinksMapper.class
        );

//        MultipleInputs.addInputPath(
//                job,
//                Utils.getPathInTemp(PagerankConfig.OUTPUT_LINKMAP),
//                SequenceFileInputFormat.class,
//                NameHashMapper.class
//        );

        job.setReducerClass(TopLinksReducer.class);
        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        Path output;

        if (isByRow) {
            output = Utils.getFinalOutputPathByKey(conf,
                    PagerankConfig.TOP_100_PATH_BY_ROW);
        } else {
            output = Utils.getFinalOutputPathByKey(conf,
                    PagerankConfig.TOP_100_PATH_BY_COL);
        }

        FileOutputFormat.setOutputPath(job, output);

        boolean ok = job.waitForCompletion(true);

        if (!ok) {
            throw new Exception("Failed at top k");
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
//        showTop(conf, true);

        System.out.println(" temp/pagerankvalue1/-r-00000".contains
                (PagerankConfig.OUTPUT_PAGERANK+1));

        System.out.println(conf.get("fs.defaultFS"));


    }
}
