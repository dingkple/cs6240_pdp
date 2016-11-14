package Preprocess;

import org.apache.hadoop.io.ArrayWritable;

/**
 * Created by kingkz on 11/13/16.
 */
public class GraphKeyArrayWritable extends ArrayWritable {
    public GraphKeyArrayWritable(GraphKeyWritable[] graphkeys) {
        super(GraphKeyWritable.class);
        set(graphkeys);
    }

    public GraphKeyArrayWritable(){
        super(GraphKeyWritable.class);
        set(new GraphKeyWritable[0]);
    }
}