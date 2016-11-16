package Pagerank;

import Config.PagerankConfig;
import Multiplication.*;
import Preprocess.MatricesGenerator;
import Util.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.net.URI;

import static TOPK.GetTopLinks.showTop;

/**
 * Created by kingkz on 11/13/16.
 */
public class RunPagerank {


    private static void iterationWithPartitionByRowCol(Configuration conf,
                                                       boolean isByRow) throws
            Exception {
        //todo

        long numberOfLinks = Long.valueOf(Utils
                .readData(PagerankConfig.NUMBER_OF_LINKS, conf));
        conf.setLong(PagerankConfig.NUMBER_OF_LINKS, numberOfLinks);

        conf.setBoolean(PagerankConfig.PARTITION_BY_ROW, isByRow);

        for (int i = 1; i <= 10; i++) {
            conf.setInt(PagerankConfig.ITER_NUM, i);
            double last = Double.parseDouble
                    (Utils.readData(PagerankConfig.DANGLING_FILENAME, conf));
            System.out.println("Dangling sum: " + last);
            if (i > 1) {
                conf.setDouble(PagerankConfig.DANGLING_NAME, last);
            }

            conf.setInt(PagerankConfig.ITER_NUM, i);
            Job job = Job.getInstance(conf);

            job.setJarByClass(RunPagerank.class);

            Path output;
            if (!isByRow) {
                MultipleInputs.addInputPath(
                        job,
                        Utils.getPathInTemp(PagerankConfig.MAPPED_OUTPUT + "/" +
                                PagerankConfig.OUTPUT_OUTLINKS_MAPPED),
                        SequenceFileInputFormat.class,
                        ByRowMapper.class
                );

            } else {
                MultipleInputs.addInputPath(
                        job,
                        Utils.getPathInTemp(PagerankConfig.MAPPED_OUTPUT + "/" +
                                PagerankConfig.OUTPUT_INLINKS_MAPPED),
                        SequenceFileInputFormat.class,
                        ByRowMapper.class
                );

            }
            output = Utils.getPathInTemp(
                    PagerankConfig.OUTPUT_PAGERANK + String.valueOf(i+1)
            );

            if (conf.get(PagerankConfig.URI_ROOT) != null) {
                String path = PagerankConfig
                        .OUTPUT_PAGERANK + String.valueOf(i);
                if (i == 1) {
                    path += "/-r-00000";
                } else {
                    path += "/part-r-00000";
                }
                job.addCacheFile(new URI(path));
            }

            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(DoubleWritable.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(DoubleWritable.class);

            job.setReducerClass(ByRowReducer.class);
            job.setNumReduceTasks(1);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            Utils.CheckOutputPath(conf, output);
            FileOutputFormat.setOutputPath(job, output);

            boolean ok = job.waitForCompletion(true);
            if (!ok) {
                throw  new Exception("Multiplication Failed");
            }

        }
    }

    public static void collectValueFromCols(Configuration conf, int iter)
            throws Exception {
        Job job = Job.getInstance(conf);

        job.setJarByClass(RunPagerank.class);
        job.setMapperClass(PagerankByColMapper.class);
        job.setReducerClass(PagerankByColReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        Path input = Utils.getPathInTemp
                (PagerankConfig.OUTPUT_PAGERANK + String.valueOf(2 * iter));
        FileInputFormat.addInputPath(job, input);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);

        Path output = Utils.getPathInTemp
                (PagerankConfig.OUTPUT_PAGERANK + String.valueOf(2 * iter+1));

        Utils.CheckOutputPath(conf, output);
        FileOutputFormat.setOutputPath(job, output);

        boolean ok = job.waitForCompletion(true);
        if (!ok) {
            throw  new Exception("Multiplication Failed");
        }

    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Path input;

        // If output is a URI, like hdfs:// or s3://, save it to conf
        // or if it's a local path, save it to conf too.
        if(otherArgs.length > 1) {
            input = new Path(otherArgs[0]);
            String op = otherArgs[1];
            if (op.startsWith("hdfs") || op.startsWith("s3")) {
                conf.set(PagerankConfig.URI_ROOT, op);
            }
            conf.set(PagerankConfig.FINAL_OUTPUT, op);
        } else if (otherArgs.length > 0) {
            input = new Path(otherArgs[0]);
            conf.set(PagerankConfig.FINAL_OUTPUT, "output");
        } else {
            input = new Path("data0");
            conf.set(PagerankConfig.FINAL_OUTPUT, "output");
        }


        long start = System.nanoTime();
        MatricesGenerator.preprocess(conf, input);
        long preprocess = System.nanoTime();
        iterationWithPartitionByRowCol(conf, true);
        long iteration = System.nanoTime();
        showTop(conf, true);
        long showTop = System.nanoTime();

        iterationWithPartitionByRowCol(conf, false);

        long iteration2 = System.nanoTime();
        showTop(conf, false);
        long end = System.nanoTime();

        printTime(conf, 1, start, preprocess, iteration, showTop);
        printTime(conf, 2, showTop, showTop, iteration2, end);
    }


    private static void printTime(
            Configuration conf,
            long iterName,
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

        Utils.writeStringToFinalPath(value, Utils.getFinalOutputPathByKey(conf,
                PagerankConfig.TIME_USED_KEY + iterName), conf);
    }

    private static double getTimeUsed(long t1, long t2) {
        return (t2 - t1) / 1000000000.;
    }
}
