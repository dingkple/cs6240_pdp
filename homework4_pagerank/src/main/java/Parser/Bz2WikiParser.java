package Parser;

import org.xml.sax.*;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.*;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
//import scala.collection.immutable.List;

/** Decompresses bz2 file and parses Wikipages on each line. */
public class Bz2WikiParser {
    private static Pattern linkPattern;

    static {
        // Keep only html filenames ending relative paths and not containing tilde (~).
        linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");
    }

    private final SAXParserFactory spf;

    public Bz2WikiParser() throws SAXException, ParserConfigurationException {
        spf = SAXParserFactory.newInstance();
        spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);

    }

    public static void main(String[] args) {
//        if (args.length != 1) {
//            System.out.println("Input bz2 file required on command line.");
//            System.exit(1);
//        }
//
//        BufferedReader reader = null;
//        try {
//            File inputFile = new File(args[0]);
//            if (!inputFile.exists() || inputFile.isDirectory() || !inputFile.getName().endsWith(".bz2")) {
//                System.out.println("Input File does not exist or not bz2 file: " + args[0]);
//                System.exit(1);
//            }
//
//            long start = System.nanoTime();
//
//            Bz2WikiParser b = new Bz2WikiParser();
//            Set<String> linkPageNames = new HashSet<>();
//            // Parser fills this list with linked page names.
//            XMLReader xmlReader = b.createParser();
//
//            BZip2CompressorInputStream inputStream = new BZip2CompressorInputStream(new FileInputStream(inputFile));
//            reader = new BufferedReader(new InputStreamReader(inputStream));
//            String line;
//            while ((line = reader.readLine()) != null) {
//                // Each line formatted as (Wiki-page-name:Wiki-page-html).
//                b.processLine(line);
//            }
//
//            long end = System.nanoTime();
//
//            System.out.println((end - start) / 1000000000L);
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            try { reader.close(); } catch (IOException e) {}
//        }
    }

    public List<String> processLine(String pageName, String line) throws ParserConfigurationException, SAXException {
//        int delimLoc = line.indexOf(':');
//        if (delimLoc <= 0) {
//            return null;
//        }
//        String pageName = line.substring(0, delimLoc);
//        String html = line.substring(delimLoc + 1);
//        Matcher matcher = namePattern.matcher(pageName);
//        if (!matcher.find()) {
//            // Skip this html file, name contains (~).
//            return null;
//        }

        // Parse page and fill li-st of linked pages.
        List<String> rst;
        try {
            Set<String> links = new HashSet<>();
            XMLReader parser = createParser(links);
            rst = new ArrayList<>();
            parser.parse(new InputSource(new StringReader(line)));
            links.remove(pageName);
            rst.addAll(links);

        } catch (Exception e) {
            // Discard ill-formatted pages.
            return null;
        }

//         Occasionally print the page and its links.
//        if (Math.random() < .01f) {
//            System.out.println(rst.toString());
//        }


        return rst;
    }

    public XMLReader createParser(Set<String> rst) throws SAXException, ParserConfigurationException {
        // Configure parser.
        SAXParser saxParser = spf.newSAXParser();
        XMLReader xmlReader = saxParser.getXMLReader();
        xmlReader.setContentHandler(new WikiParser(rst));
        return xmlReader;
    }

    /** Parses a Wikipage, finding links inside bodyContent div element. */
    private static class WikiParser extends DefaultHandler {
        /** List of linked pages; filled by parser. */
        private Set<String> linkPageNames;
        /** Nesting depth inside bodyContent div element. */
        private int count = 0;

        public WikiParser(Set<String> linkPageNames) {
            super();
            this.linkPageNames = linkPageNames;
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
            super.startElement(uri, localName, qName, attributes);
            if ("div".equalsIgnoreCase(qName) && "bodyContent".equalsIgnoreCase(attributes.getValue("id")) && count == 0) {
                // Beginning of bodyContent div element.
                count = 1;
            } else if (count > 0 && "a".equalsIgnoreCase(qName)) {
                // Anchor tag inside bodyContent div element.
                count++;
                String link = attributes.getValue("href");
                if (link == null) {
                    return;
                }
                try {
                    // Decode escaped characters in URL.
                    link = URLDecoder.decode(link, "UTF-8");
                } catch (Exception e) {
                    // Wiki-weirdness; use link as is.
                }
                // Keep only html filenames ending relative paths and not containing tilde (~).
                Matcher matcher = linkPattern.matcher(link);
                if (matcher.find()) {
                    linkPageNames.add(matcher.group(1));
                }
            } else if (count > 0) {
                // Other element inside bodyContent div.
                count++;
            }
        }

        @Override
        public void endElement(String uri, String localName, String qName) throws SAXException {
            super.endElement(uri, localName, qName);
            if (count > 0) {
                // End of element inside bodyContent div.
                count--;
            }
        }
    }
}