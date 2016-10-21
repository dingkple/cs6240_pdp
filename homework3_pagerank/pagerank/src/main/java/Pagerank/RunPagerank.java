package Pagerank;

import MapReduce.*;
import TopLinks.DescendingKeyComparator;
import TopLinks.TopMapper;
import TopLinks.TopReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
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


    // Global counter for number of records and number of sink
    public static enum UpdateCounter{
        NUMBER_OF_RECORD,
        NUMBER_OF_DANGLING
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Path input;
        Path output;

        // If output is a URI, like hdfs:// or s3://, save it to conf
        // or if it's a local path, save it to conf too.
        if(otherArgs.length > 1) {
            input = new Path(otherArgs[0]);
            String op = otherArgs[1];
            if (op.startsWith("hdfs") || op.startsWith("s3")) {
                conf.set(Config.URI_ROOT, op);
            }
            conf.set(Config.FINAL_OUTPUT, op);
        } else if (otherArgs.length > 0) {
            input = new Path(otherArgs[0]);
            conf.set(Config.FINAL_OUTPUT, "output");
        } else {
            input = new Path("data");
            conf.set(Config.FINAL_OUTPUT, "output");
        }

        output = getOutputPathByIterNum(0);


        // Pre-processing file, translate them to adjacent lists
        long startTime = System.nanoTime();
        checkPath(conf);
        readAndIterate(conf, input, output);
        long prePareTime = System.nanoTime();

        // Iteration of pagerank
        lastOutput = iteratePagerank(conf);
        long iterationTime = System.nanoTime();

        // Output top 100 links
        if (lastOutput != null) {
            showTop(conf, lastOutput, getFinalOutputPathByKey(conf, Config.TOP_100_PATH));
        }
        long getTopTime = System.nanoTime();

        // Output time used for each process
        printTime(conf, startTime, prePareTime, iterationTime, getTopTime);
    }

    private static void checkPath(Configuration conf) throws IOException {
        Utils.prepareOutputPath(conf);
        getFinalOutputPathByKey(conf, Config.TOP_100_PATH);
        getFinalOutputPathByKey(conf, Config.TIME_USED_KEY);
    }


    private static void printTime(
            Configuration conf,
            long startTime,
            long prePareTime,
            long iterationTime,
            long getTopTime) throws IOException {
        String value = String.format(
                "Time used for preparing: %.2f\n Time used for iteration: %.2f\n" +
                        "Time used for Top 100: %.2f\n",

                getTimeUsed(startTime, prePareTime),
                getTimeUsed(prePareTime, iterationTime),
                getTimeUsed(iterationTime, getTopTime)
        );
        Utils.writeStringToFinalPath(value, getFinalOutputPathByKey(conf, Config.TIME_USED_KEY), conf);
    }

    private static double getTimeUsed(long t1, long t2) {
        return (t2 - t1) / 1000000000.;
    }


    private static void showTop(Configuration conf, Path input, Path output) throws Exception {
        Job job = Job.getInstance(conf, "show_Top");

        job.setJarByClass(RunPagerank.class);
        job.setMapperClass(TopMapper.class);
        job.setReducerClass(TopReducer.class);
        job.setSortComparatorClass(DescendingKeyComparator.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, input);
        FileInputFormat.setInputDirRecursive(job, true);

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
        job.setReducerClass(PrepareReducer.class);

        job.setMapOutputKeyClass(LinkPoint.class);
        job.setMapOutputValueClass(LinkPointArrayWritable.class);
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

        // Save number of links and number of DANGLING_NAME links
        Counter numberOfRec = job.getCounters().findCounter(UpdateCounter.NUMBER_OF_RECORD);
        Counter numberOfSink = job.getCounters().findCounter(UpdateCounter.NUMBER_OF_DANGLING);

        conf.set(Utils.numberOfRecords, String.valueOf(numberOfRec.getValue()));
        conf.set(Utils.numberOfDangling, String.valueOf(numberOfSink.getValue()));

        Utils.writeData(Utils.numberOfDangling, String.valueOf(numberOfSink.getValue()), conf);
    }

    private static Path iteratePagerank(Configuration conf) throws Exception {

        Path last = null;

        for (int i = 1; i < Config.iterationNumber + 1; i++) {
            conf.set(Config.ITER_NUM, String.valueOf(i));

            Job job = Job.getInstance(conf, "iter1");

            job.setMapOutputKeyClass(LinkPoint.class);
            job.setMapOutputValueClass(CombineWritable.class);
            job.setOutputKeyClass(LinkPoint.class);
            job.setOutputValueClass(LinkPointArrayWritable.class);

            job.setJarByClass(RunPagerank.class);

            job.setMapperClass(IterateMapper.class);
            job.setReducerClass(IterateReducer.class);
            job.setPartitionerClass(IteratePartitioner.class);

            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            Path input = getOutputPathByIterNum((i+1) % 2);
            FileInputFormat.addInputPath(job, input);
            FileInputFormat.setInputDirRecursive(job, true);

            if (i > 1) {
                job.addCacheFile(Utils.getPathInTemp(Utils.totalDanglingWeight).toUri());
            }

            Path output = getOutputPathByIterNum(i % 2);
            Utils.CheckOutputPath(conf, output);
            FileOutputFormat.setOutputPath(job, output);

            last = output;

            boolean ok1 = job.waitForCompletion(true);
            if (!ok1) {
                throw new Exception("Job failed");
            }
        }
        return last;
    }

    private static Path getOutputPathByIterNum(int i) {
        return Utils.getPathInTemp(Config.OUTPUT_ROOT_PATH + "/output_" + i);
    }

    private static Path getFinalOutputPathByKey(Configuration conf, String key) throws IOException {
        Path path = new Path(conf.get(Config.FINAL_OUTPUT));
        Utils.ensureFinalPath(conf, path, false);
        Path newPath = new Path(conf.get(Config.FINAL_OUTPUT) + "/" + key);
        Utils.CheckFinalOutputPath(conf, newPath);
        return newPath;
    }
}
