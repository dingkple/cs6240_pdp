package Pagerank;

import MapReduce.*;
import TopLinks.DescendingKeyComparator;
import TopLinks.TopMapper;
import TopLinks.TopReducer;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;


/**
 * Created by kingkz on 10/16/16.
 */
public class RunPagerank {

    public static Path lastOutput;

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Path input;
        if(otherArgs.length > 0) {
            input = new Path(otherArgs[0]);
        } else {
            input = new Path("data");
        }

        long startTime = System.nanoTime();

        prepareOutputPath(conf);


        Path output = getOutputPath(0);

//        prepareOutputPath(conf);


//        readAndIterate(conf, input, output);

        long prePareTime = System.nanoTime();

        lastOutput = iteratePagerankNew(conf);

        long iterationTime = System.nanoTime();

        if (lastOutput != null) {
            showTop(conf, lastOutput, new Path("top_link_new"));
        }

        long getTopTime = System.nanoTime();

        printTime(conf, startTime, prePareTime, iterationTime, getTopTime);

//        showTopForAll(conf);
    }



    private static void printTime(Configuration conf, long startTime, long prePareTime, long iterationTime, long getTopTime) throws IOException {
        String value = String.format(
                "Time used for preparing: %.2f\n Time used for iteration: %.2f\n" +
                        "Time used for Top 100: %.2f\n",

                getTimeUsed(startTime, prePareTime),
                getTimeUsed(prePareTime, iterationTime),
                getTimeUsed(iterationTime, getTopTime)
        );

        Utils.writeData(Config.TIME_USED_KEY, value, conf);
    }

    private static double getTimeUsed(long t1, long t2) {
        return (t2 - t1) / 1000000000.;
    }

    private static void prepareOutputPath(Configuration conf) throws IOException {

        FileSystem fs = FileSystem.get(conf);
        Path outputRoot = new Path(Config.outputPathRoot);
        if (!fs.exists(outputRoot)) {
            fs.mkdirs(outputRoot);
        }
    }

    private static void showTopForAll(Configuration conf) throws Exception {
        for (int i = 0; i < Config.iterationNumber; i++) {
            showTop(conf, new Path("output_2_new_" + i), new Path("top_links_new_" + i));
        }
    }




    private static void showTop(Configuration conf, Path input, Path output) throws Exception {
        Job job = Job.getInstance(conf, "show_Top");

        job.setJarByClass(RunPagerank.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(TopMapper.class);
        job.setReducerClass(TopReducer.class);
        job.setNumReduceTasks(1);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setSortComparatorClass(DescendingKeyComparator.class);

        FileInputFormat.addInputPath(job, input);
        FileInputFormat.setInputDirRecursive(job, true);

        Utils.CheckOutputPath(conf, output);
        FileOutputFormat.setOutputPath(job, output);

        boolean ok = job.waitForCompletion(true);
        if (!ok) {
            throw new Exception("Job failed");
        }
    }


    // TODO: 10/17/16 read file to sequence and start iterate
    private static void readAndIterate(Configuration conf, Path input, Path output) throws Exception {
        Job job = Job.getInstance(conf, "read input");

        job.setJarByClass(RunPagerank.class);

        job.setMapperClass(PrepareMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(LinkPoint.class);
        job.setOutputValueClass(LinkPointArrayWritable.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, input);
        FileInputFormat.setInputDirRecursive(job, true);

        Utils.CheckOutputPath(conf, output);
        FileOutputFormat.setOutputPath(job, output);

        boolean ok = job.waitForCompletion(true);
        if (!ok) {
            throw new Exception("Job failed");
        }

    }

    private static Path iteratePagerankNew(Configuration conf) throws Exception {

        Path last = null;

        for (int i = 1; i < Config.iterationNumber + 1; i++) {
            conf.set("iter_num", String.valueOf(i));

            Job job1 = Job.getInstance(conf, "iter1");
            job1.setMapOutputKeyClass(LinkPoint.class);
            job1.setMapOutputValueClass(CombineWritable.class);
            job1.setOutputKeyClass(LinkPoint.class);
            job1.setOutputValueClass(LinkPointArrayWritable.class);

            job1.setJarByClass(RunPagerank.class);

            job1.setMapperClass(IterateMapper.class);
            job1.setReducerClass(IterateReducer.class);
            job1.setInputFormatClass(SequenceFileInputFormat.class);

            job1.setOutputFormatClass(SequenceFileOutputFormat.class);

            Path input = getOutputPath((i+1) % 2);
            FileInputFormat.addInputPath(job1, input);
            FileInputFormat.setInputDirRecursive(job1, true);

            Path output = getOutputPath(i % 2);
            Utils.CheckOutputPath(conf, output);
            FileOutputFormat.setOutputPath(job1, output);

            last = output;

            boolean ok1 = job1.waitForCompletion(true);
            if (!ok1) {
                throw new Exception("Job failed");
            }
        }
        return last;

    }

    private static Path getOutputPath(int i) {
        return new Path(Config.outputPathRoot + "/output_" + i);
    }


}
