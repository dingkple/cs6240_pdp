import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

object MmulSparse {
    val on_csv = """\s*,\s*""".r

    def flatten(raw: RDD[String], isRow: Boolean): RDD[(Double, (Seq[(Double,
        Double)]))]
    = {
        val rst = raw.map(line => on_csv.split(line))
            .map(line => line.map(v => v.toDouble))
            .map(rec => {
                if (isRow) {
                    (rec(0), Seq[(Double, Double)]((rec(1), rec(2))))
                } else {
                    (rec(1), Seq[(Double, Double)]((rec(0), rec(2))))
                }
            })
            .reduceByKey(_ ++ _)
        rst
//
//            if (isRow) {
//                rst.flatMap {
//                    case (j, data) => {
//                        data.map(rec => {
//                            (j, data)
//                        })
//                    }
//                }
//            } else {
//                rst
//            }
    }

    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("mmul").setMaster("local")
        val sc = new SparkContext(conf)

        val textA = sc.textFile("_test/dataA.csv")
        val textB = sc.textFile("_test/dataB.csv")

        val rowsA = flatten(textA, isRow = true)
        val rowsB = flatten(textB, isRow = false)

        //        val textA1 = rowsA.flatMap(rec => {
        //            rec._2.map(v => {
        //                ((rec._1, v._1), (1, rec._2))
        //            })
        //        })
        //
        //        textA1.saveAsTextFile("output/texta1")
        //
        //        val textB1 = rowsB.flatMap(rec => {
        //            rec._2.map(v => {
        //                ((v._1, rec._1), (2, rec._2))
        //            })
        //        })
        //
        //        textB1.saveAsTextFile("output/textb1")
        val textC = rowsA.flatMap{
            case (i, data) => {
                data.map( v =>{
                    (v._1, (i, data))
                })
            }
        }.join(rowsB).flatMap{
            case (col, data) => {
                val rowdata = data._1._2.toMap
                val coldata = data._2.toMap
                val common = rowdata.keySet.intersect(coldata.keySet)
                var value = 0.0
                val buf = new ListBuffer[(Int, Int, Double)]()
                for (c <- common) {
                    value += rowdata(c) * coldata(c)
                }
                buf.append((data._1._1.toInt, col.toInt, value))
                buf
            }
        }.map(rec => {
            rec._1 + ", " + rec._2 + ", " + rec._3
        }).sortBy(rec => {
            rec
        }
        , ascending = true, 1)


        // TODO: calculate C = A * B

        //        val textC =
        textC.saveAsTextFile("output")

        sc.stop()
    }
}

// vim: set ts=4 sw=4 et:
