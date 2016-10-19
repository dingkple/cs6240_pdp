package Parser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URLDecoder;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.xml.sax.*;
import org.xml.sax.helpers.DefaultHandler;

/** Decompresses bz2 file and parses Wikipages on each line. */
public class Bz2WikiParser {
    private static Pattern namePattern;
    private static Pattern linkPattern;
    static int validkeycounter = 0;

    static {
        // Keep only html pages not containing tilde (~).
        namePattern = Pattern.compile("^([^~]+)$");
        // Keep only html filenames ending relative paths and not containing tilde (~).
        linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Input bz2 file required on command line.");
            System.exit(1);
        }

//        int linecounter = 0;

        BufferedReader reader = null;
        try {
            File inputFile = new File(args[0]);
            if (!inputFile.exists() || inputFile.isDirectory() || !inputFile.getName().endsWith(".bz2")) {
                System.out.println("Input File does not exist or not bz2 file: " + args[0]);
                System.exit(1);
            }

            List<String> linkPageNames = new LinkedList<>();
            XMLReader xmlReader = createParser(linkPageNames);
            // Parser fills this list with linked page names.

            BZip2CompressorInputStream inputStream = new BZip2CompressorInputStream(new FileInputStream(inputFile));
            reader = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            while ((line = reader.readLine()) != null) {
                // Each line formatted as (Wiki-page-name:Wiki-page-html).
//                linecounter += 1;
                processLine(line, xmlReader, linkPageNames);

            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try { reader.close(); } catch (IOException e) {}

//            System.out.format("%d %d\n", linecounter, validkeycounter);
        }
    }

    public static String processLine(String line, XMLReader xmlReader, List<String> linkPageNames) {
        int delimLoc = line.indexOf(':');
        String pageName = line.substring(0, delimLoc);
        String html = line.substring(delimLoc + 1);
        Matcher matcher = namePattern.matcher(pageName);
        if (!matcher.find()) {
            // Skip this html file, name contains (~).
//            System.out.println(String.format("Invalid: %s", pageName));
            return "";
        }

        // Parse page and fill li-st of linked pages.
        linkPageNames.clear();
        try {
            xmlReader.parse(new InputSource(new StringReader(html)));
//            List<String> jsoupList = new ArrayList<>();
//            jsoupTest(html, jsoupList);
//            HashSet<String> set1 = new HashSet<>(linkPageNames);
//            HashSet<String> set2 = new HashSet<>(jsoupList);
//
//            if (set1.size() != set2.size()) {
//                System.out.format("%b s1.size: %d s2.size: %d\n",
//                        set1.equals(set2),
//                        set1.size(),
//                        set2.size());
//                System.out.println(line);
//            }

            validkeycounter += 1;

        } catch (Exception e) {
            // Discard ill-formatted pages.
//            System.out.println(html);
//            e.printStackTrace();
            return "";
        }

        // Occasionally print the page and its links.
//        if (Math.random() < .01f) {
//            System.out.println(pageName + " - " + linkPageNames);
//        }

        return pageName;
    }

    public static XMLReader createParser(List<String> linkPageNames) throws SAXException, ParserConfigurationException {
        // Configure parser.
        SAXParserFactory spf = SAXParserFactory.newInstance();
        spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
        SAXParser saxParser = spf.newSAXParser();
        XMLReader xmlReader = saxParser.getXMLReader();
        xmlReader.setContentHandler(new WikiParser(linkPageNames));
        return xmlReader;
    }

    /** Parses a Wikipage, finding links inside bodyContent div element. */
    private static class WikiParser extends DefaultHandler {
        /** List of linked pages; filled by parser. */
        private List<String> linkPageNames;
        /** Nesting depth inside bodyContent div element. */
        private int count = 0;

        public WikiParser(List<String> linkPageNames) {
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