package Pagerank;

import MapReduce.LinkPoint;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * Created by kingkz on 10/17/16.
 */
public class Config {

    public static final String DANGLING_NAME = "~";
    public static final double PAGERANK_D = 0.85;

    public static final int TOP_NUMBER = 100;

    public static final int iterationNumber = 10;

    public static final String OUTPUT_ROOT_PATH = "temp_output";
    public static final String TIME_USED_KEY = "time_used";
    public static final String ENTROPY_NAME = "~~";
    public static final String ITER_NUM = "iter_num";
    public static final String TOP_100_PATH = "top_100_links";
    public static final String URI_ROOT = "uri_root";
    public static final String TEMP_ROOT = "temp";
    public static final String FINAL_OUTPUT = "final_output";
}
