package Models.GeoAnalysis

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext

/**
  * Created by kple on 12/3/16.
  */
object MissAnalysis {
  def checkMissing(sc: SparkContext, fileSystem: FileSystem): Unit = {
    val data = sc.textFile("analysis")
      .map(row => {
        row.substring(1, row.length - 1).split(",").map(_.toInt)
      })
      .filter(row => {
        row.last > 5000
      })
      .foreach(row =>
        println(row.foldLeft("") { case (left, b) => left + " " + b.toString}))
  }
}

