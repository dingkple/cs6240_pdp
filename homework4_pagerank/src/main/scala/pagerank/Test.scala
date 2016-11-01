package pagerank

import java.util.Random

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

//import scala.util.Random

/**
  * Created by kingkz on 10/28/16.
  */
object Test {


    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Pagerank").setMaster("local[*]")
        val sc = new SparkContext(conf)
        val nums = sc.parallelize(List(1.0, 4.0, 7.0))
        val count = nums.count

        val rand = new Random()

        var ct = sc.longAccumulator("test")

        var nm:RDD[(Double, Double)] = nums.map(num => (num, 1.))


        for (ii <- 1 to 3) {
            println("ITERATION - " + ii)

            val average: Double = nums.sum / count

            println("AVERAGE - " + average)

            nm = nm.map(num => {
                println("MAP CALL IN ITERATION - " + ii)
                Console.println("In map now: " + ii)
                ct.add(1)

                if (rand.nextBoolean) {
                    (num._1, average + ii)
                }
                else {
                    (num._1, average + ii)
                }
            }).keyBy(_._1).reduceByKey((a, b) => (a._1 + b._2, a._2 + b._1)).map(k => (k._2._1, k._2._1))
        }

        println("OUTPUT")

        nm.foreach(println)

        println("counter1: " + ct.value)

        println("ADD 1")

        nm.foreach(num => {ct.add(1)})

        println("counter2: " + ct.value)


    }
}
