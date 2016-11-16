package Preprocess;

import Config.PagerankConfig;
import Util.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
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
    }


    private static void getLinkMap(Configuration conf, Path path) throws
            Exception {
        Job job = Job.getInstance(conf, "get_link_map");

        job.setJarByClass(MatricesGenerator.class);
        job.setMapperClass(LinkNameMapMapper.class);
        job.setPartitionerClass(LinkNamePartitioner.class);
        job.setReducerClass(LinkNameMapReducer.class);

        job.setMapOutputKeyClass(GraphKeyWritable.class);
        job.setMapOutputValueClass(GraphKeyArrayWritable.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, path);
        FileInputFormat.setInputDirRecursive(job, true);

        Path linksOutput = new Path(PagerankConfig.TEMP_ROOT);

        Utils.CheckOutputPath(conf, linksOutput);

        MultipleOutputs.addNamedOutput(
                        job,
                        PagerankConfig.OUTPUT_OUTLINKS,
                SequenceFileOutputFormat.class,
                GraphKeyWritable.class,
                TextCellArrayWritable.class
                );

        MultipleOutputs.addNamedOutput(
                job,
                PagerankConfig.OUTPUT_INLINKS,
                SequenceFileOutputFormat.class,
                GraphKeyWritable.class,
                TextCellArrayWritable.class
        );

        MultipleOutputs.addNamedOutput(
                job,
                PagerankConfig.OUTPUT_LINKMAP,
                SequenceFileOutputFormat.class,
                Text.class,
                IntWritable.class
        );

        MultipleOutputs.addNamedOutput(
                job,
                PagerankConfig.OUTPUT_PAGERANK,
                SequenceFileOutputFormat.class,
                IntWritable.class,
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

        Utils.writeData(
                PagerankConfig.NUMBER_OF_LINKS,
                String.valueOf(job
                        .getCounters()
                        .findCounter(PagerankConfig.PagerankCounter.LINK_COUNTER)
                        .getValue()),
                conf);

        writeMappedGraph(conf);
    }

    public static void writeMappedGraph(Configuration conf) throws
            Exception {

        Job job = Job.getInstance(conf);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(CellArrayWritable.class);

        if (conf.get(PagerankConfig.URI_ROOT) != null) {
            job.addCacheFile(Utils.getPathInTemp(PagerankConfig.OUTPUT_LINKMAP)
                    .toUri());
        }

        job.setJarByClass(MatricesGenerator.class);
//        job.setReducerClass(NameToNumberReducer.class);
        job.setNumReduceTasks(0);

        MultipleInputs.addInputPath(
                job,
                Utils.getPathInTemp(PagerankConfig.OUTPUT_OUTLINKS),
                SequenceFileInputFormat.class,
                NameToNumberMapper.class
        );

        MultipleInputs.addInputPath(
                job,
                Utils.getPathInTemp(PagerankConfig.OUTPUT_INLINKS),
                SequenceFileInputFormat.class,
                NameToNumberMapper.class
        );

        Utils.CheckOutputPath(conf, Utils.getPathInTemp(PagerankConfig
                .OUTPUT_INLINKS_MAPPED));
        Utils.CheckOutputPath(conf, Utils.getPathInTemp(PagerankConfig
                .OUTPUT_OUTLINKS_MAPPED));

        MultipleOutputs.addNamedOutput(
                job,
                PagerankConfig.OUTPUT_INLINKS_MAPPED,
                SequenceFileOutputFormat.class,
                IntWritable.class,
                CellArrayWritable.class
        );

        MultipleOutputs.addNamedOutput(
                job,
                PagerankConfig.OUTPUT_OUTLINKS_MAPPED,
                SequenceFileOutputFormat.class,
                IntWritable.class,
                CellArrayWritable.class
        );

        Path output = new Path(Utils.getPathInTemp(PagerankConfig
                .MAPPED_OUTPUT).toString());
        Utils.CheckOutputPath(conf, output);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileOutputFormat.setOutputPath(job, output);

        if (!job.waitForCompletion(true)) {
            throw new Exception("Failed at mapping: ");
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        preprocess(conf, new Path("data0"));
    }
}
