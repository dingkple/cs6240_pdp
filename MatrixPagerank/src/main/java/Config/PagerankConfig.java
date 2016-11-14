package Config;

import org.apache.hadoop.io.Text;

/**
 * Created by kingkz on 11/11/16.
 */
public class PagerankConfig {

    public static final String OUTPUT_NAME_MAP = "name_map";
    public static final String OUTPUT_OUTLINKS = "outlinks";
    public static final String OUTPUT_INLINKS = "inlinks";
    public static final int OUTLINK_TYPE = 1;
    public static final int INLINK_TYPE = 2;
    public static final Long KEY_TYPE_COL = 2L;
    public static final String OUTPUT_INLINKS_MAPPED = "inlinkmapped";
    public static final String OUTPUT_OUTLINKS_MAPPED = "outlinkmapped";
    public static final String OUTPUT_LINKMAP = "linkmap2long";
    public static final String RAW_LINK_GRAPH = "rawlinkgraph";
    public static final String OUTPUT_LINK_GRAPH = "linkgraph";
    public static final String OUTPUT_DANGLING = "outputdangling";
    public static final String OUTPUT_PAGERANK = "pagerankvalue";
    public static final String NUMBER_OF_LINKS = "numberoflinks";
    public static final String PAGERANK_COL = "~~";
    public static final String DANGLING_FILENAME = "danglingsum";
    public static Long KEY_TYEP_ROW = 1L;
    public static int PR_BLOCK_SIZE = 5000;
    public static Long ROWCOL_BLOCK_SIZE_LONG = 5000L;
    public static String ROWCOL_BLOCK_SIZE_STRING = "rowcolblocksize";

    public static enum PagerankCounter{
        LINK_COUNTER,
        EDGE_COUNTER,
        NUMBER_OF_DANGLING
    }

    public static final String DANGLING_NAME = "~";
    public static final double PAGERANK_D = 0.85;

    public static final int TOP_NUMBER = 100;

    public static final int iterationNumber = 10;

    public static final String OUTPUT_ROOT_PATH = "temp_output";
    public static final String TIME_USED_KEY = "time_used";
    public static final String ENTROPY_NAME = "~~";
    public static final String ITER_NUM = "iter_num";
    public static final String TOP_100_PATH = "top_100_links";
    public static final String URI_ROOT = "uri_root";
    public static final String TEMP_ROOT = "temp";
    public static final String FINAL_OUTPUT = "final_output";
    public static final String NON_DANGLE = "non_dangle";
    public static String NEW_TOTAL_WEIGHT = "new_total_weight";

}
