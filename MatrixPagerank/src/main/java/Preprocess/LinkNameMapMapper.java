package Preprocess;

import Config.PagerankConfig;
import com.google.common.collect.Iterables;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.*;

import static Preprocess.Bz2WikiParser.createParser;
import static Preprocess.Bz2WikiParser.processLine;

/**
 * Created by kingkz on 11/11/16.
 */
public class LinkNameMapMapper extends Mapper<LongWritable, Text, GraphKeyWritable,
        GraphKeyArrayWritable> {

    private XMLReader xmlReader;
    private Set<String> linkPageNames;

    private HashMap<String, List<GraphKeyWritable>> inlinkMap;


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

            if (linkPageNames.contains(pageName)) {
                linkPageNames.remove(pageName);
            }

            GraphKeyWritable linkKey = new GraphKeyWritable(
                    PagerankConfig.OUTLINK_TYPE,
                    pageName,
                    linkPageNames.size());

            List<GraphKeyWritable> outlinks = new ArrayList<>();
            if (pageName.length() > 0) {

                for (String name : linkPageNames) {
                    if (!inlinkMap.containsKey(name)) {
                        inlinkMap.put(name, new ArrayList<>());
                    }
                    inlinkMap.get(name).add(linkKey);
                    outlinks.add(new GraphKeyWritable(
                            PagerankConfig.OUTLINK_TYPE,
                            name,
                            linkPageNames.size()
                    ));
                }

                context.write(new GraphKeyWritable(PagerankConfig
                                .OUTLINK_TYPE,
                                pageName),
                        new GraphKeyArrayWritable(Iterables.toArray(
                                outlinks,
                                GraphKeyWritable.class
                        )));
                context.write(new GraphKeyWritable(PagerankConfig.INLINK_TYPE,
                        pageName),
                        new GraphKeyArrayWritable());

                context.write(new GraphKeyWritable(PagerankConfig.LINK_MAP_TYPE,
                        pageName),
                        new GraphKeyArrayWritable());
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (String name : inlinkMap.keySet()) {
            context.write(
                    new GraphKeyWritable(
                            PagerankConfig.INLINK_TYPE, name),
                    new GraphKeyArrayWritable(
                                Iterables.toArray(
                                        inlinkMap.get(name),
                                        GraphKeyWritable.class
                                )
                    )
            );

            context.write(
                    new GraphKeyWritable(
                            PagerankConfig.LINK_MAP_TYPE,
                            name),
                    new GraphKeyArrayWritable()
                    );


            context.write(new GraphKeyWritable(PagerankConfig.OUTLINK_TYPE,
                    name), new GraphKeyArrayWritable());
        }
    }
}
