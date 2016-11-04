package pagerank

/**
  * Created by kingkz on 10/28/16.
  */
import org.apache.hadoop.fs.{Path}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

import java.util.regex.Pattern

import Parser.Bz2WikiParser
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer

sealed case class MyData(linkName:String, pagerank: Double, outlinks: Set[String])

object Pagerank {

    type JDoc = org.jsoup.nodes.Document

    val linkPattern:Pattern = Pattern.compile("^\\..*/([^~]+)\\.html$")
    val namePattern:Pattern = Pattern.compile("^([^~]+)$")
    val parser = new Bz2WikiParser

    def getAllOutlinks(pagename:String, content: String): Set[String] = {
        val links = parser.processLine(pagename, content)

//        val links = Bz2WikiParser.processLine(content)
        if (links == null || links.isEmpty) {
            Set[String]()
        } else {
            links.toSet
        }
    }

    def reverse[A, B](m: Map[A, Set[B]]) =
        m.values.toSet.flatten.map(v => (v, m.keys.filter(m(_).contains(v)))).toMap

    def main(args: Array[String]): Unit = {

        val hadoopConf = new org.apache.hadoop.conf.Configuration()

        var output:String = null;
        if (args.length > 1) {
            output = args(1)
        } else {
            output = "output"
        }

        val conf:SparkConf = new SparkConf().setAppName("Pagerank")
        var fileSystem:org.apache.hadoop.fs.FileSystem = null;
        if (output.startsWith("hdfs") || output.startsWith("s3")) {
            fileSystem = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(output), hadoopConf)
        } else {
            fileSystem = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
            conf.setMaster("local[*]");
        }

        val sc = new SparkContext(conf)

        val outputPath = new Path(output)
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true)
        }

        val allOutLinks = sc.textFile(args(0))
            .map(line => line.split(":", 2))
            .filter(line => line.length == 2 && namePattern.matcher(line(0)).find())
            .map(line => (line(0), getAllOutlinks(line(0), line(1))))
//            .map(line => (line(0), linkSequence(line(0), line(1))))
            .map(rec => MyData(rec._1, 0, rec._2)).cache()

        val setSize = allOutLinks.count()
        val totalWeight = sc.doubleAccumulator("total weight")

        // TODO: outlinks now accessable, need to calculate the pagerank and broadcast
        // TODO: problems: how to calculate total size and sum of dangling
        val broadcastLinkNum = sc.broadcast(setSize)
        var iteration:RDD[MyData] = allOutLinks
        for (iter <- 1 to 10) {
            val broadcastDangling = sc.broadcast(1.0 - totalWeight.value)
            totalWeight.reset()
            iteration = iteration.flatMap(rec => {
                    if (iter == 1) {
                        val weight = 1.0 / broadcastLinkNum.value
                        var data: ListBuffer[MyData] = new ListBuffer[MyData]()
                        if (rec.outlinks.nonEmpty) {
                            for (ol <- rec.outlinks) {
                                data += MyData(ol, weight / rec.outlinks.size, Set[String]())
                            }
                        }
                        data += MyData(rec.linkName, 0, rec.outlinks)
                        data
                    } else {
                        val dangling = broadcastDangling.value
                        val number = broadcastLinkNum.value
                        val alpha = 0.15
                        val weight: Double = alpha / number + (1 - alpha) * (dangling / number + rec.pagerank)
                        var data: ListBuffer[MyData] = new ListBuffer[MyData]()
                        if (rec.outlinks.nonEmpty) {
                            for (ol <- rec.outlinks) {
                                data += MyData(ol, weight / rec.outlinks.size, Set[String]())
                            }
                        }
                        data += MyData(rec.linkName, 0, rec.outlinks)
                        data
                    }
                }).keyBy(_.linkName).reduceByKey((a, b) => {
                    MyData(a.linkName, a.pagerank+b.pagerank, a.outlinks.union(b.outlinks))
                }).map(rec => rec._2).cache()
//            }).reduce( (a, b) => MyData(a.linkName, a.pagerank+b.pagerank, a.outlinks.union(b.outlinks)))

            iteration.foreach(x => {
                if (x.outlinks.nonEmpty) {
                    totalWeight.add(x.pagerank)
                }
            })
        }

        val lastTotal = sc.broadcast(totalWeight.value)
        val result = iteration.map(rec =>
            {
                val dangling = 1 - lastTotal.value
                val number = broadcastLinkNum.value
                val alpha = 0.15
                val weight:Double = alpha/number + (1-alpha) * (dangling/number + rec.pagerank)
                (weight, rec.linkName)
            }
        ).takeOrdered(100)(Ordering[Double].reverse.on(_._1))

        println("Total size: " + setSize)

        sc.makeRDD(result).repartition(1).saveAsTextFile(output)
    }
}