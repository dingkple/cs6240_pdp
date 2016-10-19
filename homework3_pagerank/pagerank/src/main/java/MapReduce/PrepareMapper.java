package MapReduce;

import Pagerank.RunPagerank;
import Pagerank.Utils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static Parser.Bz2WikiParser.createParser;
import static Parser.Bz2WikiParser.processLine;

/**
 * Created by kingkz on 10/17/16.
 */
public class PrepareMapper extends Mapper<LongWritable, Text, LinkPoint, LinkPointArrayWritable> {

    XMLReader xmlReader;
    List<String> linkPageNames;
    HashSet<String> nameSet;
    private ArrayList<LinkPoint> linkList;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try {
            linkPageNames = new ArrayList<>();
            xmlReader = createParser(linkPageNames);
            nameSet = new HashSet<>();
            linkList = new ArrayList<>();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        }

    }

    public void map(LongWritable _K, Text line, Context context) throws IOException, InterruptedException {
        if (xmlReader != null) {

            String pageName = processLine(line.toString(), xmlReader, linkPageNames);
            LinkPoint lp1 = new LinkPoint();
            if (pageName.length() > 0) {
                lp1.setLineName(pageName);
                lp1.clear();
                nameSet.add(pageName);
                context.getCounter(RunPagerank.UpdateCounter.NUMBER_OF_RECORD).increment(1);

                if (linkPageNames.size() > 0) {
                    linkList.clear();
                    for (String name : linkPageNames) {
                        if (!name.equals(pageName))
                            linkList.add(new LinkPoint(name, 0, 0));
//                    nameSet.add(name);
                    }
                    context.write(lp1, new LinkPointArrayWritable(linkList));
                } else {
                    context.getCounter(RunPagerank.UpdateCounter.NUMBER_OF_SINK).increment(1);
                }

            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Pagerank.Utils.writeData(
                Utils.numberOfRecords,
                String.valueOf(nameSet.size()),
                context.getConfiguration());
    }
}
