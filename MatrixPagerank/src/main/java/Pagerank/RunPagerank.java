package Pagerank;

import Config.PagerankConfig;
import Multiplication.MatricesMapper;
import Multiplication.MultiplicationReducer;
import Multiplication.PRValueV1Mapper;
import Multiplication.ROWCOLWritable;
import Preprocess.CellArrayWritable;
import Preprocess.MatricesGenerator;
import Util.Utils;
import org.apache.commons.math3.util.DoubleArray;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 * Created by kingkz on 11/13/16.
 */
public class RunPagerank {


    private static void iteration(Configuration conf) throws Exception {
        //todo

        long numberOfLinks = Long.valueOf(Utils
                .readData(PagerankConfig.NUMBER_OF_LINKS, conf));
        conf.setLong(PagerankConfig.NUMBER_OF_LINKS, numberOfLinks);
        conf.setInt(PagerankConfig.ROWCOL_BLOCK_SIZE_STRING, (int) (numberOfLinks /
                        PagerankConfig.ROWCOL_BLOCK_SIZE_LONG));

        for (int i = 1; i <= 10; i++) {
            if (i > 1) {
                conf.setDouble(PagerankConfig.DANGLING_NAME,
                        Double.parseDouble(Utils.readData
                        (PagerankConfig.DANGLING_NAME, conf)));
            }
            conf.setInt(PagerankConfig.ITER_NUM, i);
            Job job = Job.getInstance(conf);

            job.setJarByClass(RunPagerank.class);
            MultipleInputs.addInputPath(
                    job,
                    Utils.getPathInTemp(PagerankConfig.OUTPUT_PAGERANK
                            + String.valueOf(i%2)),
                    SequenceFileInputFormat.class,
                    PRValueV1Mapper.class
            );

            MultipleInputs.addInputPath(
                    job,
                    Utils.getPathInTemp(PagerankConfig.OUTPUT_INLINKS),
                    SequenceFileInputFormat.class,
                    MatricesMapper.class
            );

//            job.setOutputKeyClass(IntWritable.class);
//            job.setOutputValueClass(CellArrayWritable.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(ROWCOLWritable.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(DoubleWritable.class);

            job.setReducerClass(MultiplicationReducer.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            Path output = Utils.getPathInTemp
                    (PagerankConfig.OUTPUT_PAGERANK + String.valueOf((i+1)%2));
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
        }
    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
//        MatricesGenerator.preprocess(conf, new Path("data0"));
        iteration(conf);
    }
}
