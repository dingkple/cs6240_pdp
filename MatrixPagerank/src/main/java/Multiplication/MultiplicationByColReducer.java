package Multiplication;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created by kingkz on 11/13/16.
 */
public class MultiplicationByColReducer extends Reducer<IntWritable,
        ROWCOLArrayWritable, IntWritable, DoubleWritable> {



}
