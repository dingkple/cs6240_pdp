package TOPK;

import Config.PagerankConfig;
import Util.Utils;
import org.apache.hadoop.conf.Configuration;
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

        if (isByRow) {
            pagerankInput = Utils.getPathInTemp(PagerankConfig.OUTPUT_PAGERANK + "11");
        } else {
            pagerankInput = Utils.getPathInTemp(PagerankConfig.OUTPUT_PAGERANK + "21");
        }

        MultipleInputs.addInputPath(
                job,
                pagerankInput,
                SequenceFileInputFormat.class,
                TopLinksMapper.class
        );

        MultipleInputs.addInputPath(
                job,
                Utils.getPathInTemp(PagerankConfig.OUTPUT_LINKMAP),
                SequenceFileInputFormat.class,
                NameHashMapper.class
        );

        job.setReducerClass(TopLinksReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(PagerankCellWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        Path output = new Path(PagerankConfig.FINAL_OUTPUT);
        Utils.CheckOutputPath(conf, output);

        FileOutputFormat.setOutputPath(job, output);

        boolean ok = job.waitForCompletion(true);

        if (!ok) {
            throw new Exception("Failed at top k");
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        showTop(conf, true);
    }
}
