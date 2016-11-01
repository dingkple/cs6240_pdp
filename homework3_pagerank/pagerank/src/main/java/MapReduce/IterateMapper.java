package MapReduce;

import Pagerank.Config;
import Pagerank.Utils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.net.URI;

/**
 * Created by kingkz on 10/17/16.
 */
public class IterateMapper extends Mapper<LinkPoint, LinkPointArrayWritable, LinkPoint, CombineWritable> {


    // Initial page rank of every link
    private static double oriWeight;

    private double totalDanglingPagerank;

    // For converge, not used in the final phrase, since we need to do 10 iterations, just for anoter option
    private double entropy;

    // Dummy keys to calculate entropy and DANGLING_NAME pagerank sum.
    private LinkPoint dummyEntropy;
    private LinkPoint dummyDanglingLink;


    private Long numberOfLinks;


    // Sum of dummyDanglingLink weights in this mapper
    private double currentDangling;

    // Number of iteration
    private int iterNumber;
    private double newTotalWeight;
    private double sump2;
    private double sump1;
    private double sumkeyweight;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        iterNumber = Integer.valueOf(context.getConfiguration().get(Config.ITER_NUM));

        numberOfLinks = Long.valueOf(context.getConfiguration().get(Utils.numberOfRecords));

        // initial pagerank
        oriWeight = 1./ numberOfLinks;

        // If this is the first iteration, DANGLING_NAME links' page rank = numberOfDangling * initial weight
        // If not, read from the cached file written by reducer of last iteration
        if (iterNumber > 1) {
            totalDanglingPagerank = readDanglingWeight(context);
        }

        dummyDanglingLink = new LinkPoint(Config.DANGLING_NAME, 0, 0);
        dummyEntropy = new LinkPoint(Config.ENTROPY_NAME, 0, 0);
        currentDangling = 0;
        newTotalWeight = 0;
        sump1 = 0;
        sump2 = 0;
        sumkeyweight = 0;
    }


    /**
     * read data from file
     * @param ctx context
     * @return sum of DANGLING_NAME weights in last iteration, if no file existing, return 0 (There's something wrong, should
     * never happen)
     * @throws IOException
     */
    private double readDanglingWeight(Context ctx) throws IOException {
        URI[] uriArray = ctx.getCacheFiles();

        Path path = null;
        if (uriArray != null) {
            for (URI uri : uriArray) {
                if (uri.getPath().contains(Utils.totalDanglingWeight)) {
                    path = new Path(uri.getPath());
                    break;
                }
            }
        }

        if (path == null) {
            return Double.valueOf(Utils.readData(Utils.totalDanglingWeight, ctx.getConfiguration()));
        }

        FileSystem fs = FileSystem.get(ctx.getConfiguration());
        FSDataInputStream fin = fs.open(path);
        String line = fin.readUTF();
        String[] data = line.split("=");
        return Double.valueOf(data[1]);
    }

    /**
     * Key is a link, mapper will map pagerank change to each of its outlinks, and map a dummy link containing its
     * adjacent link to keep the structure.
     * @param key a link. NOTE that the weight here is not the real weight, in each page rank iteration, mapper will
     *            read all the information needed from last iteration and calculate the real pagerank here, then send
     *            the pagerank changes to its reducer
     * @param value A list of outlinks
     * @param context Context of mapper
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LinkPoint key, LinkPointArrayWritable value, Context context) throws IOException, InterruptedException {
        Writable[] array = value.get();

        double keyWeight;
        if (iterNumber == 1) {
            keyWeight = oriWeight;
        } else {

            //calculate real pagerank here, it's the actual pagerank we should use in this iteration
            double alpha = Config.PAGERANK_D;
            keyWeight = key.getWeight();
            sumkeyweight += keyWeight;


            double p1 = alpha * (keyWeight + totalDanglingPagerank / numberOfLinks);
            double p2 = (1 - alpha) / numberOfLinks;
            keyWeight = p1 + p2;
            sump1 += p1;
            sump2 += p2;
        }
        newTotalWeight += keyWeight;

        entropy += keyWeight * Math.log(keyWeight) / Math.log(2);

        // To keep the structure of key link
        CombineWritable cb = new CombineWritable(value, 0);
        LinkPoint carpKey = new LinkPoint(key);
        carpKey.clear();
        context.write(carpKey, cb);

        // Map the changes to outlinks
        double change = 0;
        if (array.length > 0) {
            change = keyWeight / array.length;
        } else {
            currentDangling += keyWeight;
        }
        for (Writable lp : array) {
            LinkPoint l = (LinkPoint) lp;
            l.clear();
            context.write(l, new CombineWritable(change));
        }
    }

    /**
     * Sen the sum of weights of DANGLING_NAME links in this mapper
     * @param context Context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(dummyEntropy, new CombineWritable(entropy));
        context.write(dummyDanglingLink, new CombineWritable(currentDangling));
        Utils.writeData(
                Config.NEW_TOTAL_WEIGHT + "_" + iterNumber,
                String.valueOf(newTotalWeight),
                context.getConfiguration());
        Utils.writeData(
                "sump1" + "_" + iterNumber,
                String.valueOf(sump1),
                context.getConfiguration());
        Utils.writeData(
                "sump2" + "_" + iterNumber,
                String.valueOf(sump2),
                context.getConfiguration());
        Utils.writeData(
                "sumkeyweight" + "_" + iterNumber,
                String.valueOf(sumkeyweight),
                context.getConfiguration());
    }
}
