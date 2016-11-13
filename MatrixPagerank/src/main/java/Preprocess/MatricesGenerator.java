package Preprocess;

import Config.PagerankConfig;
import Util.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 * Created by kingkz on 11/11/16.
 */
public class MatricesGenerator {



    public static void preprocess(Configuration conf, Path input) throws
            Exception {
        getLinkMap(conf, input);

        getGraph(conf);
    }

    private static void getGraph(Configuration conf) throws Exception {

        Job job = Job.getInstance(conf, "get_link_graph");

        job.setJarByClass(MatricesGenerator.class);
        job.setMapperClass(GraphCreaterMapper.class);
        job.setReducerClass(GraphCreaterReducer.class);

        job.setMapOutputKeyClass(GraphKeyWritable.class);
        job.setMapOutputValueClass(CellArrayWritable.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(CellArrayWritable.class);

        Path input = Utils.getPathInTemp(PagerankConfig
                .RAW_LINK_GRAPH);
        FileInputFormat.addInputPath(job, input);

        job.setInputFormatClass(SequenceFileInputFormat.class);

        Path output = Utils.getPathInTemp(PagerankConfig.OUTPUT_LINK_GRAPH);
        Utils.CheckOutputPath(conf, output);

        FileOutputFormat.setOutputPath(job, output);

        MultipleOutputs.addNamedOutput(
                job,
                PagerankConfig.OUTPUT_OUTLINKS_MAPPED,
                SequenceFileOutputFormat.class,
                LongWritable.class,
                CellArrayWritable.class
        );

        MultipleOutputs.addNamedOutput(
                job,
                PagerankConfig.OUTPUT_INLINKS_MAPPED,
                SequenceFileOutputFormat.class,
                LongWritable.class,
                CellArrayWritable.class
        );

        MultipleOutputs.addNamedOutput(
                job,
                PagerankConfig.OUTPUT_DANGLING,
                SequenceFileOutputFormat.class,
                LongWritable.class,
                NullWritable.class
        );

        boolean ok = job.waitForCompletion(true);
        if (!ok) {
            throw  new Exception("Job Fail at getting link's map");
        }

    }

    private static void getLinkMap(Configuration conf, Path path) throws
            Exception {
        Job job = Job.getInstance(conf, "get_link_map");

        job.setJarByClass(MatricesGenerator.class);
        job.setMapperClass(LinkNameMapMapper.class);
        job.setReducerClass(LinkNameMapReducer.class);

        job.setMapOutputKeyClass(GraphKeyWritable.class);
        job.setMapOutputValueClass(TextArrayWritable.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, path);
        FileInputFormat.setInputDirRecursive(job, true);

        Path linkmapOutput = Utils.getPathInTemp(PagerankConfig
                .OUTPUT_LINKMAP);

        Utils.CheckOutputPath(conf, linkmapOutput);

        Path linksOutput = new Path(PagerankConfig.TEMP_ROOT);

        Utils.CheckOutputPath(conf, linksOutput);

        MultipleOutputs.addNamedOutput(
                        job,
                        PagerankConfig.RAW_LINK_GRAPH,
                SequenceFileOutputFormat.class,
                GraphKeyWritable.class,
                TextArrayWritable.class
                );

        MultipleOutputs.addNamedOutput(
                job,
                PagerankConfig.OUTPUT_LINKMAP,
                SequenceFileOutputFormat.class,
                Text.class,
                LongWritable.class
        );

        MultipleOutputs.addNamedOutput(
                job,
                PagerankConfig.OUTPUT_PAGERANK,
                SequenceFileOutputFormat.class,
                LongWritable.class,
                DoubleWritable.class
        );

        FileOutputFormat.setOutputPath(job, linksOutput);

        boolean ok = job.waitForCompletion(true);
        if (!ok) {
            throw  new Exception("Job Fail at getting link's map");
        }

        System.out.println("Counter : " + job.getCounters().findCounter
                (PagerankConfig
                .PagerankCounter.LINK_COUNTER).getValue());
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        preprocess(conf, new Path("data0"));

    }
}
