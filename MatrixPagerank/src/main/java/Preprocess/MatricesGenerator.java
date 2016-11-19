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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.net.URI;

/**
 * Created by kingkz on 11/11/16.
 */
public class MatricesGenerator {



    public static void preprocess(Configuration conf, Path input) throws
            Exception {
        getLinkMap(conf, input);
        writeMappedGraph(conf);
    }


    private static void getLinkMap(Configuration conf, Path path) throws
            Exception {
        Job job = Job.getInstance(conf, "get_link_map");

        job.setJarByClass(MatricesGenerator.class);
        job.setMapperClass(LinkNameMapMapper.class);
        job.setPartitionerClass(LinkNamePartitioner.class);
        job.setReducerClass(LinkNameMapReducer.class);
//        job.setNumReduceTasks(PagerankConfig.NUMBER_OF_REDUCERS_INT);

        job.setMapOutputKeyClass(GraphKeyWritable.class);
        job.setMapOutputValueClass(GraphKeyArrayWritable.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, path);
        FileInputFormat.setInputDirRecursive(job, true);

        Path linksOutput = Utils.getPathInTemp(conf, "");

        Utils.CheckOutputPath(conf, linksOutput);

        System.out.println(linksOutput.toString());

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

    }

    public static void writeMappedGraph(Configuration conf) throws
            Exception {

        Job job = Job.getInstance(conf);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(CellArrayWritable.class);

        URI links = new URI(conf.get(PagerankConfig.OUTPUT_WORKING_DIRECTORY) +
                "/" +
                Utils.getPathInTemp(conf,
                        PagerankConfig
                                .OUTPUT_LINKMAP + "/-r-00000")
                        .toString());

//        if (conf.get(PagerankConfig.URI_ROOT) != null) {
//            System.out.println("adding cache: " + links.toString());
//            job.addCacheFile(links);
//        }

        job.setJarByClass(MatricesGenerator.class);
//        job.setReducerClass(NameToNumberReducer.class);
        job.setNumReduceTasks(0);

        MultipleInputs.addInputPath(
                job,
                Utils.getPathInTemp(conf, PagerankConfig.OUTPUT_OUTLINKS),
                SequenceFileInputFormat.class,
                NameToNumberMapper.class
        );

        MultipleInputs.addInputPath(
                job,
                Utils.getPathInTemp(conf, PagerankConfig.OUTPUT_INLINKS),
                SequenceFileInputFormat.class,
                NameToNumberMapper.class
        );

        Utils.CheckOutputPath(conf, Utils.getPathInTemp(conf, PagerankConfig
                .OUTPUT_INLINKS_MAPPED));
        Utils.CheckOutputPath(conf, Utils.getPathInTemp(conf, PagerankConfig
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

        Path output = new Path(Utils.getPathInTemp(conf, PagerankConfig
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
