package pagerank

import java.util.regex.Pattern

import Parser.Bz2WikiParser
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

/**
  * Created by kingkz on 11/2/16.
  */
object Pagerank {
    val linkPattern:Pattern = Pattern.compile("^\\..*/([^~]+)\\.html$")
    val namePattern:Pattern = Pattern.compile("^([^~]+)$")
    val parser = new Bz2WikiParser

    //Extracting the outlinks, done in the parser class in hadoop version.
    def getAllOutlinks(pagename:String, content: String): Seq[String] = {
        val links = parser.processLine(pagename, content)
        if (links == null || links.isEmpty) {
            Seq[String]()
        } else {
            links.toSet.toSeq
        }
    }

    /*
    Calculate the time used for each period
     */
    def getSeconds(end:Long, start:Long):Double = {
        (end - start) / 1000000000.0
    }

    /*
    PreProcess the raw RDD file, parse each line to filter the qualified
    pageNames and their outlinks. Things done here was implemented in
    pre-pressing class in hadoop version.
     */
    def preProcessing(rawRdd:RDD[String]): RDD[(String, Seq[String])] = {
        rawRdd
            // The following three lines did the job of pre-processing
            // Mapper's job.
            .map(line => line.split(":", 2))
            .filter(rec => rec.length==2 && namePattern.matcher(rec(0)).find())
            .map(rec => (rec(0), getAllOutlinks(rec(0), rec(1))))
            // The flatmap is finding all the links only seen in one of the
            // outlinks, preparing for the iteration.
            .flatMap {
                case (link, outs) =>
                    var data = new ListBuffer[(String, Seq[String])]()
                    for (out <- outs) {
                        data += ((out, Seq[String]()))
                    }
                    data += ((link, outs))
                    data
            }
            // Links only seen in the outlinks will appear here with empty
            // outlink-list, they will be considered as dangling nodes too.
            .reduceByKey(_.union(_)).cache()
    }

    /*
    Iteration 10 times to get the pagerank value.
     */
    def pagerankIteration
    (linkRDD: RDD[(String, Seq[String])], sparkContext: SparkContext)
    : RDD[(String, Double)]= {
        // Broadcast the number of links, this value was stored in the hadoop
        // .configuration
        val linkSize = sparkContext.broadcast(linkRDD.count())

        // Base value for pagerank
        var pr:RDD[(String, Double)] = linkRDD.map(rec => (rec._1, 1.0 /
            linkSize.value))

        // Calculate the sum of all the non-dangling links' weight, it was
        // done in the pagerank reduer in hadoop version
        val receivedWeight = sparkContext.doubleAccumulator("received")

        for (iter <- 1 to 10) {

            // This value was written to file in the reducer of
            // pagerankIteration, then add to distributed file cache and
            // read by all the mappers in next iteration in hadoop version.
            val received = sparkContext.broadcast(receivedWeight.value)
            receivedWeight.reset()
            println("Dangling: " + (1 - received.value))

            // Hadoop version does not have this JOIN.
            // FlatMap is the pagerank mapper, reduceByKey and rightOuterJoin
            // are the pagerank reducer in hadoop version.
            val rst = pr.join(linkRDD).values.flatMap{
                case (weight, outs) => outs.map(out => (out, weight/outs.size))
            }.reduceByKey((a, b) => a + b).rightOuterJoin(linkRDD)
//            rst.filter(rec => rec._2._1.nonEmpty && rec._2._2.nonEmpty)
            // A minor difference about calculating dangling links'weight
            rst.filter(rec => rec._2._1.nonEmpty)
                .foreach(rec => {
                    receivedWeight.add(rec._2._1.get)
                })

            // Calculate all the non-dangling links' pagerank and broadcast
            val sumWeight = sparkContext.broadcast(receivedWeight.value)

            // In my hadoop version, this mapper was combined with previous
            // mapper, I will calculate the real pagerank in the mapper, then
            // map the weight-change to its outlinks.
            pr = rst.map(rec => {
                var weight = 0.85 * (1-sumWeight.value) / linkSize.value +
                    0.15/linkSize.value
                if (rec._2._1.nonEmpty) {
                    weight += rec._2._1.get * 0.85
                }
                (rec._1, weight)
            }).cache()
        }
        pr
    }

    def main(args: Array[String]): Unit = {
        // HadoopConfiguration, used to manipulate the file path and directories
        val hadoopConf = new org.apache.hadoop.conf.Configuration()

        if (args.isEmpty) {
            println("No input file")
            System.exit(-1)
        }
2

        var output:String = null
        if (args.length > 1) {
            output = args(1)
        } else {
            output = "output"
        }

        // Check if it should run on local mode or cluster mode.
        val conf:SparkConf = new SparkConf().setAppName("Pagerank")
        var fileSystem:org.apache.hadoop.fs.FileSystem = null
        if (output.startsWith("hdfs") || output.startsWith("s3")) {
            fileSystem = org.apache.hadoop.fs.FileSystem.get(
                new java.net.URI(output), hadoopConf)
        } else {
            fileSystem = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
            conf.setMaster("local[*]")
        }

        val sparkContext = new SparkContext(conf)

        // Check if the output directories exists
        val outputPath = new Path(output)
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true)
        }

        // Start pre-processing
        val preprocessedTime:Long = System.nanoTime()
        val allLinks = preProcessing(sparkContext.textFile(args(0)))

        // Start iteration
        val iterationTime:Long = System.nanoTime()
        val lastPR = pagerankIteration(allLinks, sparkContext)

        // Save the TOP 100 links into file
        val topKTime:Long = System.nanoTime()
        sparkContext.makeRDD(lastPR.takeOrdered(100)
        (Ordering[Double].reverse.on(x=>x._2)))
            .repartition(1).sortBy(rec => rec._2, ascending = false, 1)
                .map(rec => (rec._2, rec._1))
            .saveAsTextFile(output)

        // Print the time used for each step
        val endTime:Long = System.nanoTime()
        println(lastPR.count())
        println("Process: " + getSeconds(iterationTime, preprocessedTime))
        println("Iteration: " + getSeconds(topKTime, iterationTime))
        println("TopK : " + getSeconds(endTime, topKTime))
    }
}
