package Preprocess;

import Config.PagerankConfig;
import com.google.common.collect.Iterables;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.yarn.webapp.view.HtmlPage;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static Preprocess.Bz2WikiParser.createParser;
import static Preprocess.Bz2WikiParser.processLine;

/**
 * Created by kingkz on 11/11/16.
 */
public class LinkNameMapMapper extends Mapper<LongWritable, Text, GraphKeyWritable,
        TextArrayWritable> {

    private XMLReader xmlReader;
    private Set<String> linkPageNames;

    private HashMap<String, Set<String>> inlinkMap;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try {
            linkPageNames = new HashSet<>();
            xmlReader = createParser(linkPageNames);
            inlinkMap = new HashMap<>();
        } catch (SAXException | ParserConfigurationException e) {
            e.printStackTrace();
        }

    }

    public void map(LongWritable _K, Text line, Context context) throws
            IOException, InterruptedException {
        if (xmlReader != null) {
            // Get the name of the link
            linkPageNames.clear();
            String pageName = processLine(line.toString(), xmlReader);
            if (pageName.length() > 0) {
                linkPageNames.add(pageName);
                if (linkPageNames.contains(pageName)) {
                    linkPageNames.remove(pageName);
                }

                for (String name : linkPageNames) {
                    if (!inlinkMap.containsKey(name)) {
                        inlinkMap.put(name, new HashSet<>());
                    }
                    inlinkMap.get(name).add(pageName);
                }
                context.write(new GraphKeyWritable(PagerankConfig
                                .OUTLINK_TYPE,
                                pageName),
                        new TextArrayWritable(linkPageNames));
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (String name : inlinkMap.keySet()) {
            context.write(new GraphKeyWritable(PagerankConfig
                    .INLINK_TYPE, name), new TextArrayWritable(inlinkMap.get
                    (name)));

            context.write(new GraphKeyWritable(PagerankConfig.OUTLINK_TYPE,
                    name), new TextArrayWritable());
        }
    }
}
