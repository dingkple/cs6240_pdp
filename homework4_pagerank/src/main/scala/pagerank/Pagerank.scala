package pagerank

/**
  * Created by kingkz on 10/28/16.
  */
import java.io.File
import java.nio.file.{Files, Paths}

import org.apache.hadoop.fs.FileUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.jsoup.Jsoup

import scala.collection.JavaConversions._
import java.net.{URLDecoder}
import java.util.regex.{Pattern}

import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

sealed case class MyData(linkName:String, pagerank: Double, outlinks: Set[String])

object Pagerank {

    type JDoc = org.jsoup.nodes.Document

    val linkPattern:Pattern = Pattern.compile("^\\..*/([^~]+)\\.html$")
    val namePattern:Pattern = Pattern.compile("^([^~]+)$")

    def linkSequence(pagename:String, content: String): Set[String] = {
        val doc = Jsoup.parse(content)
        val body = doc.select("div[id=bodyContent]")
        if (body != null) {
            val links = body.select("a[href]")
                .map(elem => URLDecoder.decode(elem.attr("href"), "UTF-8"))
                .map(absLink => linkPattern.matcher(absLink))
                .filter(matcher => matcher.find())
                .map(matcher => matcher.group(1))
                .filter(x => !x.equals(pagename))
                .toSet
            links
        } else {
            Set[String]()
        }
    }

    def reverse[A, B](m: Map[A, Set[B]]) =
        m.values.toSet.flatten.map(v => (v, m.keys.filter(m(_).contains(v)))).toMap

    def getListOfFiles(dir: String):List[String] = {
        val d = new File(dir)
        if (d.exists && d.isDirectory) {
            d.listFiles.filter(_.isFile).map(_.getPath).toList
        } else {
            List[String]()
        }
    }


    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Pagerank").setMaster("local[*]")
        val sc = new SparkContext(conf)

        if (Files.exists(Paths.get("output"))) {
            FileUtil.fullyDelete(new File("output"))
        }

        if (Files.exists(Paths.get("output1"))) {
            FileUtil.fullyDelete(new File("output1"))
        }

        val linkNum = sc.longAccumulator("number of links")
        val danglingPagerank = sc.doubleAccumulator("weight of dangling")
        val totalWeight = sc.doubleAccumulator("total weight")
        val ct = sc.longAccumulator("counter")

        val allInputs = getListOfFiles(args(0))
        println(allInputs)

        val allOutLinks = sc.textFile(allInputs.mkString(","))
            .map(line => line.split(":", 2))
            .filter(line => namePattern.matcher(line(0)).find())
            .map(line => (line(0), linkSequence(line(0), line(1))))
            .map(rec => {

                MyData(rec._1, 1.0 / rec._2.size, rec._2)
            })

        val k = allOutLinks.map(x=>{
            val buff = x.outlinks + x.linkName
            buff
        }).reduce((x, y) => x.union(y))
        val setSize = k.count(x=>x.nonEmpty)

        allOutLinks.saveAsTextFile("output1")
        // TODO: outlinks now accessable, need to calculate the pagerank and broadcast
        // TODO: problems: how to calculate total size and sum of dangling
        val broadcastLinkNum = sc.broadcast(setSize)
        var iteration:RDD[MyData] = allOutLinks
        for (iter <- 1 to 10) {
//            val broadcastDangling = sc.broadcast(danglingPagerank.value)
//            val broadcastTotal = sc.broadcast(totalWeight.value)
            val broadcastDangling = sc.broadcast(1.0 - totalWeight.value)
            val dvalue = broadcastDangling.value;
//            val tvalue = broadcastTotal.value;
            println("weight: " + totalWeight.value + " "
                + (1 - totalWeight.value) + " " + broadcastLinkNum.value + " "
                + broadcastDangling.value + " " + broadcastDangling.value
                + setSize)
            danglingPagerank.reset()
            totalWeight.reset()
            iteration = iteration.flatMap(rec => {
//                ct.add(1)
                if (iter == 1) {
                    val weight = 1.0 / broadcastLinkNum.value
                    var data: ListBuffer[MyData] = new ListBuffer[MyData]()
                    if (rec.outlinks.nonEmpty) {
                        for (ol <- rec.outlinks) {
                            data += MyData(ol, weight/rec.outlinks.size, Set[String]())
                        }
//                    } else {
//                        data += MyData("~", weight, Set[String]())
                    }
                    data += MyData(rec.linkName, 0, rec.outlinks)
                    data
                } else {
                    val dangling = broadcastDangling.value
                    val number = broadcastLinkNum.value
                    val alpha = 0.15
                    val weight:Double = alpha/number + (1-alpha) * (dangling/number + rec.pagerank)
                    var data: ListBuffer[MyData] = new ListBuffer[MyData]()
                    if (rec.outlinks.nonEmpty) {
                        for (ol <- rec.outlinks) {
                            data += MyData(ol, weight/rec.outlinks.size, Set[String]())
                        }
//                    } else {
//                        data += MyData("~", weight, Set[String]())
                    }
                    data += MyData(rec.linkName, 0, rec.outlinks)
                    data
                }
            }).keyBy(_.linkName).reduceByKey((a, b) => {
                MyData(a.linkName, a.pagerank+b.pagerank, a.outlinks.union(b.outlinks))
            }).map(rec => {
                    rec._2
                })

            iteration.foreach(x => {
//                if (!x.linkName.equals("~") && x.outlinks.nonEmpty) {
//                    totalWeight.add(x.pagerank)
//                } else {
//                    danglingPagerank.add(x.pagerank)
//                }
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

        sc.makeRDD(result).repartition(1).saveAsTextFile("output")

        println("total calculated: " + ct.value)
    }
}