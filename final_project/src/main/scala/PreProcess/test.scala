package PreProcess

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by kple on 12/1/16.
  */
object test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("test")
    val spark = new SparkSession.Builder().config(conf).getOrCreate()
    val data = spark.read.format("libsvm").load("sample_libsvm_data.txt")
    data.printSchema()
    data.schema.map(l => {
      val k = 1
      println(k)
    })
  }

}
