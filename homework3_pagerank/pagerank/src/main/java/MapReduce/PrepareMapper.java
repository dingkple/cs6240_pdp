package MapReduce;

import Pagerank.RunPagerank;
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
import java.util.Set;

import static Parser.Bz2WikiParser.createParser;
import static Parser.Bz2WikiParser.processLine;

/**
 * Created by kingkz on 10/17/16.
 */
public class PrepareMapper extends Mapper<LongWritable, Text, LinkPoint, LinkPointArrayWritable> {

    XMLReader xmlReader;
    Set<String> linkPageNames;
    HashSet<String> nameSet;
    HashSet<String> nameSet2;
    private List<LinkPoint> outlinkList;
    private LinkPoint modelLink;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try {
            linkPageNames = new HashSet<>();
            xmlReader = createParser(linkPageNames);

            modelLink = new LinkPoint();

            // nameSet is the set of pageNames before ":"
            // nameSet2 is the set of pageNames after ":" (in html)
            nameSet = new HashSet<>();
            nameSet2 = new HashSet<>();
            outlinkList = new ArrayList<>();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        }

    }

    public void map(LongWritable _K, Text line, Context context) throws IOException, InterruptedException {
        if (xmlReader != null) {

            // Get the name of the link
            String pageName = processLine(line.toString(), xmlReader, linkPageNames);
            if (pageName.length() > 0) {
                modelLink.setLineName(pageName);
                modelLink.clear();

                // Number of links += 1
                nameSet.add(pageName);
                context.getCounter(RunPagerank.UpdateCounter.NUMBER_OF_RECORD).increment(1);

                outlinkList.clear();
                if (linkPageNames.size() > 0) {
                    // Filter self-loop in adjacent list. (pageName appear in its outLink)
                    // And generate outLink set
                    linkPageNames.stream().filter(name -> !name.equals(pageName)).forEach(name -> {
                        outlinkList.add(new LinkPoint(name, 0, 0));
                        nameSet2.add(name);
                    });
                }

                context.write(modelLink, new LinkPointArrayWritable(outlinkList));
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        LinkPointArrayWritable emptyListWritable = new LinkPointArrayWritable();
        for (String name : nameSet2) {
            // For pageNames only in the html, I treat them as DANGLING_NAME point too.
            if (!nameSet.contains(name)) {
                modelLink.setLineName(name);
                context.write(modelLink, emptyListWritable);
            }
        }
    }
}
