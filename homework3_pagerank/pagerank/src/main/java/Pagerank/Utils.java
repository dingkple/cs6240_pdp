package Pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by kingkz on 10/17/16.
 */
public class Utils {
    /*
        Methods in this class are mainly for manipulate the directory, written to the hdfs temp or the final output
     */


    public static final String numberOfRecords = "number_of_records";
    public static final String totalDanglingWeight = "total_sink";
    public static final String entropy = "entropt";
    public static String numberOfDangling = "number_of_sink";



    public static void writeData(String key, String value, Configuration conf) throws IOException {
        Path path = getPathInTemp(key);
        checkTempFilePathbyKey(conf, key);
        FSDataOutputStream fin = FileSystem.get(conf).create(path);
        fin.writeUTF(key+"="+value);
        fin.close();
    }

    public static String readData(String key, Configuration conf) throws IOException {
        Path path = getPathInTemp(key);

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


    public static void prepareOutputPath(Configuration conf) throws IOException {
        ensurePath(conf, new Path(Config.TEMP_ROOT), true);
        ensurePath(conf, getPathInTemp(Config.OUTPUT_ROOT_PATH), true);
    }

    private static void checkTempFilePathbyKey(Configuration conf, String key) throws IOException {
        FileSystem hdfs = FileSystem.get(conf);
        Path tempPath = getPathInTemp(key);
        if (!hdfs.exists(tempPath)) {
            hdfs.mkdirs(tempPath);
        }

        Path path = getPathInTemp(key);
        if (hdfs.exists(path)) {
            hdfs.delete(path, true);
        }
    }

    public static Path getPathInTemp(String key) {
        return new Path(Config.TEMP_ROOT+ "/" + key + ".txt");
    }


    public static void CheckOutputPath(Configuration conf, Path path) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        fs.delete(path, true);
    }


    public static FileSystem getFileSystem(Configuration conf) throws IOException {
        String uri = conf.get(Config.URI_ROOT);
        if (uri != null && uri.length() > 0) {
            try {
                return FileSystem.get(new URI(uri), conf);
            } catch (URISyntaxException e) {
                return FileSystem.get(conf);
            }
        }
        return FileSystem.get(conf);
    }

    public static void ensurePath(Configuration conf, Path path, boolean newPath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        check(fs, path, newPath);

    }

    private static void check(FileSystem fs, Path path, boolean newPath) throws IOException {
        if (fs.exists(path)) {
            if (newPath) {
                fs.delete(path, true);
                fs.mkdirs(path);
            }
        } else {
            fs.mkdirs(path);
        }
    }


    public static void CheckFinalOutputPath(Configuration conf, Path path) throws IOException {
        FileSystem fs = getFileSystem(conf);
        fs.delete(path, true);
    }


    public static void writeStringToFinalPath(String string, Path path, Configuration conf) throws IOException {
        FSDataOutputStream fin = getFileSystem(conf).create(path);
        fin.writeUTF(string);
        fin.close();
    }

    public static void ensureFinalPath(Configuration conf, Path path, boolean newPath) throws IOException {
        FileSystem fs = getFileSystem(conf);
        check(fs, path, newPath);
    }

}
