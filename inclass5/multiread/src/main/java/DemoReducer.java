import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

// Author: Nat Tuck
public class DemoReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> vals, Context ctx) {
        String name = null;
        String year = null;

        for (Text xx : vals) {
            String[] parts = xx.toString().split("=");

            if (parts[0].equals("name")) {
                name = parts[1];
            }
            
            if (parts[0].equals("year")) {
                year = parts[1];
            }

        }

        try {
            if (name != null && year != null) {
                ctx.write(new Text(year), new Text(name));
            }
        }
        catch (Exception ee) {
            ee.printStackTrace(System.err);
            throw new Error("I give up");
        }
    }
}
