package Pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;

import java.io.*;

/**
 * Created by kingkz on 10/17/16.
 */
public class Utils {

    public static final String numberOfRecords = "number_of_records";

    public static final String totalWeight = "total_weight";
    public static final String totalSink = "total_sink";
    private static final String tempPathRoot = "temp/";
    public static final String entropy = "entropt";
    public static String numberOfSink = "number_of_sink";


    public static void writeData(String key, String value, Configuration conf) throws IOException {
        Path path = getPath(key);

        checkTempPath(conf, key);

        FSDataOutputStream fin = FileSystem.get(conf).create(path);
        fin.writeUTF(key+"="+value);
        fin.close();
    }

    public static void checkTempPath(Configuration conf, String key) throws IOException {
        FileSystem hdfs = FileSystem.get(conf);
        Path tempPath = new Path(tempPathRoot);
        if (!hdfs.exists(tempPath)) {
            hdfs.mkdirs(tempPath);
        }

        Path path = getPath(key);
        if (hdfs.exists(path)) {
            hdfs.delete(path, true);
        }
    }

    private static Path getPath(String key) {
        return new Path(tempPathRoot + key + ".data");
    }

    public static String readData(String key, Configuration conf) throws IOException {
        Path path = getPath(key);

        FileSystem fs = FileSystem.get(conf);

        if (!fs.exists(path)) {
            System.out.println("No Record Number");
            return "0";
        }

        FSDataInputStream fin = fs.open(path);
        String line = fin.readUTF();
        String[] data = line.split("=");
        return data[1];
    }

    public static void CheckOutputPath(Configuration conf, Path path) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        fs.delete(path, true);
    }
}
