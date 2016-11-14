package Pagerank;

import Config.PagerankConfig;
import Multiplication.*;
import Preprocess.MatricesGenerator;
import TOPK.PagerankValueMapper;
import Util.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

import static TOPK.GetTopLinks.showTop;

/**
 * Created by kingkz on 11/13/16.
 */
public class RunPagerank {


    private static void iterationWithPartitionByRow(Configuration conf,
                                                    boolean isByRow) throws
            Exception {
        //todo

        long numberOfLinks = Long.valueOf(Utils
                .readData(PagerankConfig.NUMBER_OF_LINKS, conf));
        conf.setLong(PagerankConfig.NUMBER_OF_LINKS, numberOfLinks);
        conf.setInt(PagerankConfig.ROWCOL_BLOCK_SIZE_STRING, (int) (numberOfLinks /
                PagerankConfig.ROWCOL_BLOCK_SIZE_LONG));
        conf.setBoolean(PagerankConfig.PARTITION_BY_ROW, isByRow);

        for (int i = 1; i <= 10; i++) {
            conf.setInt(PagerankConfig.ITER_NUM, i);
            double last = Double.parseDouble
                    (Utils.readData(PagerankConfig.DANGLING_FILENAME, conf));
//            conf.setDouble(PagerankConfig.DANGLING_NAME, last);
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
                        Utils.getPathInTemp(PagerankConfig.OUTPUT_PAGERANK
                                + String.valueOf(2 * i - 1)),
                        SequenceFileInputFormat.class,
                        PRValueV1Mapper.class
                );
                MultipleInputs.addInputPath(
                        job,
                        Utils.getPathInTemp(PagerankConfig.OUTPUT_OUTLINKS),
                        SequenceFileInputFormat.class,
                        MatricesMapper.class
                );

                output = Utils.getPathInTemp(
                        PagerankConfig.OUTPUT_PAGERANK + String.valueOf(2 * i)
                );
            } else {
                MultipleInputs.addInputPath(
                        job,
                        Utils.getPathInTemp(PagerankConfig.OUTPUT_PAGERANK
                        + String.valueOf(i)),
                        SequenceFileInputFormat.class,
                        PRValueV1Mapper.class
                );
                MultipleInputs.addInputPath(
                        job,
                        Utils.getPathInTemp(PagerankConfig.OUTPUT_INLINKS),
                        SequenceFileInputFormat.class,
                        MatricesMapper.class
                );

                output = Utils.getPathInTemp(
                        PagerankConfig.OUTPUT_PAGERANK + String.valueOf(i+1)

                );
            }


            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(ROWCOLArrayWritable.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(DoubleWritable.class);

            job.setReducerClass(MultiplicationByRowReducer.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            Utils.CheckOutputPath(conf, output);
            FileOutputFormat.setOutputPath(job, output);

//            if (i > 1) {
//                job.addCacheFile(Utils.getPathInTemp(PagerankConfig
//                        .DANGLING_FILENAME).toUri());
//            }

            boolean ok = job.waitForCompletion(true);
            if (!ok) {
                throw  new Exception("Multiplication Failed");
            }

            if (!isByRow) {
                collectValueFromCols(conf, i);
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

        boolean isByRow = true;
        Configuration conf = new Configuration();
//        MatricesGenerator.preprocess(conf, new Path("data0"));
        iterationWithPartitionByRow(conf, isByRow);
        showTop(conf, isByRow);
    }
}
