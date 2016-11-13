import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object MmulSparse {
    val on_csv = """\s*,\s*""".r

    def flatten(raw: RDD[String]): RDD[(Double, (Seq[(Double,
        Double)]))]
    = {
        val rst = raw.map(line => on_csv.split(line))
            .map(line => line.map(v => v.toDouble))
            .map(rec => (rec(0), Seq[(Double, Double)]((rec(1), rec(2)))))
            .reduceByKey(_ ++ _)
        rst
    }

    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("mmul").setMaster("local")
        val sc = new SparkContext(conf)

        val textA = sc.textFile("_test/dataA.csv")
        val textB = sc.textFile("_test/dataB.csv")

        val rowsB = flatten(textB)

        val textC = textA.map(line => on_csv.split(line))
            .map(line => line.map(v => v.toDouble))
            .keyBy(rec => rec(1))
            /*
            All values in B are needed, so data need to be communicated: |B|
             */
            .join(rowsB)
            .flatMap(
                /*
                Basically every cell in A generate a row same len as rowB, so
                 its called |A| * N (N is the row number of B)
                 */
                rec => {
                val rA = rec._2._1
                val rB = rec._2._2
                rB.map(rowB => {
                    ((rA(0), rB.head._1), rA(2)*rowB._2)
                })
            })
            /*
            If communicating in reducing part counts too, the amount of data
            communicated is sum(Ai*Bi) where Ai is the length of col[i] in A
            and Bi is row[i] in B
             */
            .reduceByKey(_+_)
            .filter(rec => rec._2 != 0)
            .map(rec => {
                rec._1._1 + ", " + rec._1._2 + ", " + rec._2
            })
            .sortBy(rec => rec, ascending = true, 1)

        println("A: " + textA.count() + " B: " + textB.count() + " C: " +
            textC.count)

        textC.saveAsTextFile("output")
        sc.stop()
    }
}
